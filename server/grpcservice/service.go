// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package grpcservice

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metaservicepb"
	"github.com/CeresDB/ceresmeta/pkg/log"
	"github.com/CeresDB/ceresmeta/server/cluster"
	"github.com/CeresDB/ceresmeta/server/member"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Service struct {
	metaservicepb.UnimplementedCeresmetaRpcServiceServer
	opTimeout time.Duration
	h         Handler

	// Store as map[string]*grpc.ClientConn
	// TODO: remove unavailable connection
	connConns sync.Map
}

func NewService(opTimeout time.Duration, h Handler) *Service {
	return &Service{
		opTimeout: opTimeout,
		h:         h,
	}
}

type HeartbeatStreamSender interface {
	Send(response *metaservicepb.NodeHeartbeatResponse) error
}

// Handler is needed by grpc service to process the requests.
type Handler interface {
	GetClusterManager() cluster.Manager
	GetLeader(ctx context.Context) (*member.GetLeaderResp, error)
	UnbindHeartbeatStream(ctx context.Context, node string) error
	BindHeartbeatStream(ctx context.Context, node string, sender HeartbeatStreamSender) error
	ProcessHeartbeat(ctx context.Context, req *metaservicepb.NodeHeartbeatRequest) error

	// TODO: define the methods for handling other grpc requests.
}

type streamBinder struct {
	timeout time.Duration
	h       Handler
	stream  HeartbeatStreamSender

	// States of the binder which may be updated.
	node  string
	bound bool
}

func (b *streamBinder) bindIfNot(ctx context.Context, node string) error {
	if b.bound {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	if err := b.h.BindHeartbeatStream(ctx, node, b.stream); err != nil {
		return ErrBindHeartbeatStream.WithCausef("node:%s, err:%v", node, err)
	}

	b.bound = true
	b.node = node
	return nil
}

func (b *streamBinder) unbind(ctx context.Context) error {
	if !b.bound {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, b.timeout)
	defer cancel()
	if err := b.h.UnbindHeartbeatStream(ctx, b.node); err != nil {
		return ErrUnbindHeartbeatStream.WithCausef("node:%s, err:%v", b.node, err)
	}

	return nil
}

// NodeHeartbeat implements gRPC CeresmetaServer.
func (s *Service) NodeHeartbeat(heartbeatSrv metaservicepb.CeresmetaRpcService_NodeHeartbeatServer) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var (
		forwardedStream       metaservicepb.CeresmetaRpcService_NodeHeartbeatClient
		ceresmetaClient       metaservicepb.CeresmetaRpcServiceClient
		errCh                 chan error
		previousCtx           context.Context
		previousCancel        context.CancelFunc
		previousForwardedAddr string
	)

	binder := streamBinder{
		timeout: s.opTimeout,
		h:       s.h,
		stream:  heartbeatSrv,
	}

	defer func() {
		if err := binder.unbind(ctx); err != nil {
			log.Error("fail to unbind stream", zap.Error(err))
		}
	}()

	// Process the message from the stream sequentially.
	for {
		req, err := heartbeatSrv.Recv()
		if err == io.EOF {
			log.Warn("receive EOF and exit the heartbeat loop")
			return nil
		}
		if err != nil {
			return ErrRecvHeartbeat.WithCause(err)
		}

		// Forward request to the leader.
		{
			forwardedAddr, isLocal, err := s.getForwardedAddr(ctx)
			if err != nil {
				return errors.Wrap(err, "node heartbeat")
			}

			maybeInitForwardStream := func() (metaservicepb.CeresmetaRpcService_NodeHeartbeatClient, string, error) {
				if forwardedStream == nil || forwardedAddr != previousForwardedAddr {
					if previousCancel != nil {
						previousCancel()
					}

					if !isLocal {
						dialCtx, dialCancel := context.WithTimeout(context.Background(), s.opTimeout)
						defer dialCancel()
						ceresmetaClient, err = s.getCeresmetaClient(dialCtx, forwardedAddr)
						if err != nil {
							return nil, "", errors.Wrap(err, "node heartbeat")
						}

						if ceresmetaClient != nil {
							previousCtx, previousCancel = context.WithCancel(context.Background())
							forwardedStream, err = s.createHeartbeatForwardedStream(previousCtx, ceresmetaClient)
							if err != nil {
								previousCancel()
								return nil, "", err
							}

							previousForwardedAddr = forwardedAddr
							errCh = make(chan error, 1)
							go forwardRegionHeartbeatRespToClient(forwardedStream, heartbeatSrv, errCh)
						}
					}
				}
				return nil, "", nil
			}
			forwardedStream, previousForwardedAddr, err = maybeInitForwardStream()

			if forwardedStream != nil {
				if err := forwardedStream.Send(req); err != nil {
					return errors.Wrap(err, "node heartbeat")
				}

				select {
				case err := <-errCh:
					return err
				default:
				}
				continue
			}
		}

		if err := binder.bindIfNot(ctx, req.Info.Node); err != nil {
			log.Error("fail to bind node stream", zap.Error(err))
		}

		func() {
			ctx1, cancel := context.WithTimeout(ctx, s.opTimeout)
			defer cancel()
			err := s.h.ProcessHeartbeat(ctx1, req)
			if err != nil {
				log.Error("fail to handle heartbeat", zap.Any("heartbeat", req), zap.Error(err))
			} else {
				log.Debug("succeed in handling heartbeat", zap.Any("heartbeat", req))
			}
		}()
	}
}

// AllocSchemaID implements gRPC CeresmetaServer.
func (s *Service) AllocSchemaID(ctx context.Context, req *metaservicepb.AllocSchemaIdRequest) (*metaservicepb.AllocSchemaIdResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: ResponseHeader(err, "grpc alloc schema id")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.AllocSchemaID(ctx, req)
	}

	schemaID, err := s.h.GetClusterManager().AllocSchemaID(ctx, req.GetHeader().GetClusterName(), req.GetName())
	if err != nil {
		return &metaservicepb.AllocSchemaIdResponse{Header: ResponseHeader(err, "grpc alloc schema id")}, nil
	}

	return &metaservicepb.AllocSchemaIdResponse{
		Header: OkResponseHeader(),
		Name:   req.GetName(),
		Id:     schemaID,
	}, nil
}

// AllocTableID implements gRPC CeresmetaServer.
func (s *Service) AllocTableID(ctx context.Context, req *metaservicepb.AllocTableIdRequest) (*metaservicepb.AllocTableIdResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.AllocTableIdResponse{Header: ResponseHeader(err, "grpc alloc table id")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.AllocTableID(ctx, req)
	}

	table, err := s.h.GetClusterManager().AllocTableID(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetName(), req.GetHeader().GetNode())
	if err != nil {
		return &metaservicepb.AllocTableIdResponse{Header: ResponseHeader(err, "grpc alloc table id")}, nil
	}

	return &metaservicepb.AllocTableIdResponse{
		Header:     OkResponseHeader(),
		ShardId:    table.GetShardID(),
		SchemaName: table.GetSchemaName(),
		SchemaId:   table.GetSchemaID(),
		Name:       table.GetName(),
		Id:         table.GetID(),
	}, nil
}

// GetTables implements gRPC CeresmetaServer.
func (s *Service) GetTables(ctx context.Context, req *metaservicepb.GetTablesRequest) (*metaservicepb.GetTablesResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.GetTablesResponse{Header: ResponseHeader(err, "grpc get tables")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.GetTables(ctx, req)
	}

	tables, err := s.h.GetClusterManager().GetTables(ctx, req.GetHeader().GetClusterName(), req.GetHeader().GetNode(), req.GetShardId())
	if err != nil {
		return &metaservicepb.GetTablesResponse{Header: ResponseHeader(err, "grpc get tables")}, nil
	}

	tableMap := make(map[uint32]*metaservicepb.ShardTables, len(tables))
	for shardID, shardTables := range tables {
		for _, table := range shardTables.Tables {
			shardTablesPb, ok := tableMap[shardID]
			if ok {
				shardTablesPb.Tables = append(shardTablesPb.Tables, &metaservicepb.TableInfo{
					Id:         table.ID,
					Name:       table.Name,
					SchemaId:   table.SchemaID,
					SchemaName: table.SchemaName,
				})
			} else {
				tableMap[shardID] = &metaservicepb.ShardTables{
					Tables: []*metaservicepb.TableInfo{
						{
							Id:         table.ID,
							Name:       table.Name,
							SchemaId:   table.SchemaID,
							SchemaName: table.SchemaName,
						},
					},
				}
			}
		}
	}

	return &metaservicepb.GetTablesResponse{
		Header:    OkResponseHeader(),
		TablesMap: tableMap,
	}, nil
}

// DropTable implements gRPC CeresmetaServer.
func (s *Service) DropTable(ctx context.Context, req *metaservicepb.DropTableRequest) (*metaservicepb.DropTableResponse, error) {
	ceresmetaClient, err := s.getForwardedCeresmetaClient(ctx)
	if err != nil {
		return &metaservicepb.DropTableResponse{Header: ResponseHeader(err, "grpc drop table")}, nil
	}

	// Forward request to the leader.
	if ceresmetaClient != nil {
		return ceresmetaClient.DropTable(ctx, req)
	}

	err = s.h.GetClusterManager().DropTable(ctx, req.GetHeader().GetClusterName(), req.GetSchemaName(), req.GetName(), req.GetId())
	if err != nil {
		return &metaservicepb.DropTableResponse{Header: ResponseHeader(err, "grpc drop table")}, nil
	}

	return &metaservicepb.DropTableResponse{
		Header: OkResponseHeader(),
	}, nil
}

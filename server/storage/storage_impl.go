// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.
// Copyright 2022 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// fork from: https://github.com/tikv/pd/blob/master/server/storage/endpoint

package storage

import (
	"fmt"
	"strings"
	"time"

	"github.com/CeresDB/ceresdbproto/pkg/metapb"
	"github.com/CeresDB/ceresmeta/server/etcdutil"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/protobuf/proto"
)

// StorageImpl is the base underlying storage endpoint for all other upper
// specific storage backends. It should define some common storage interfaces and operations,
// which provides the default implementations for all kinds of storages.
type StorageImpl struct {
	KV
}

// NewStorageImpl creates a new base storage endpoint with the given KV and encryption key manager.
// It should be embedded inside a storage backend.
func NewStorageImpl(
	kv KV,
) *StorageImpl {
	return &StorageImpl{
		kv,
	}
}

// newEtcdBackend is used to create a new etcd backend.
func newEtcdStorage(client *clientv3.Client, rootPath string, requestTimeout time.Duration) *StorageImpl {
	return NewStorageImpl(
		NewEtcdKV(client, rootPath, requestTimeout))
}

func (si *StorageImpl) GetCluster(clusterId uint32, meta *metapb.Cluster) (bool, error) {
	value, err := si.Get(fmt.Sprintf("%d", clusterId))
	if err != nil || value == "" {
		return false, err
	}
	err = proto.Unmarshal([]byte(value), meta)
	if err != nil {
		return false, etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	return true, nil
}

func (si *StorageImpl) PutCluster(clusterId uint32, meta *metapb.Cluster) error {
	value, err := proto.Marshal(meta)
	if err != nil {
		return etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	return si.Put(fmt.Sprintf("%d", clusterId), string(value))
}

func (si *StorageImpl) GetClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) (bool, error) {
	value, err := si.Get(fmt.Sprintf("%d", clusterId))
	if err != nil || value == "" {
		return false, err
	}
	err = proto.Unmarshal([]byte(value), clusterMetaData)
	if err != nil {
		return false, etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	return true, nil
}

func (si *StorageImpl) PutClusterTopology(clusterId uint32, clusterMetaData *metapb.ClusterTopology) error {
	value, err := proto.Marshal(clusterMetaData)
	if err != nil {
		return etcdutil.ErrEtcdKVGet.WithCause(err)
	}
	return si.Put(fmt.Sprintf("%d", clusterId), string(value))
}

func (si *StorageImpl) GetTables(clusterId uint32, schemaId uint32, tableId []uint64, table []*metapb.Table) (bool, error) {
	for _, item := range tableId {
		key := strings.Join([]string{fmt.Sprintf("%d", clusterId), fmt.Sprintf("%d", schemaId), fmt.Sprintf("%d", item)}, delimiter)
		value, err := si.Get(key)
		if err != nil || value == "" {
			return false, err
		}
		var tableData *metapb.Table
		err = proto.Unmarshal([]byte(value), tableData)
		if err != nil {
			return false, etcdutil.ErrEtcdKVGet.WithCause(err)
		}
		table = append(table, tableData)
	}
	return true, nil
}

func (si *StorageImpl) PutTables(clusterId uint32, schemaId uint32, tables []*metapb.Table) error {
	for _, item := range tables {
		key := strings.Join([]string{fmt.Sprintf("%d", clusterId), fmt.Sprintf("%d", schemaId), fmt.Sprintf("%d", item.Id)}, delimiter)
		value, err := proto.Marshal(item)
		if err != nil {
			return etcdutil.ErrEtcdKVGet.WithCause(err)
		}
		err = si.Put(key, string(value))
		if err != nil {
			return err
		}
	}
	return nil
}

func (si *StorageImpl) DeleteTables(clusterId uint32, schemaId uint32, tableIDs []uint64) (bool, error) {
	for _, item := range tableIDs {
		key := strings.Join([]string{fmt.Sprintf("%d", clusterId), fmt.Sprintf("%d", schemaId), fmt.Sprintf("%d", item)}, delimiter)
		err := si.Delete(key)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (si *StorageImpl) GetShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) (bool, error) {
	for _, item := range shardId {
		key := strings.Join([]string{fmt.Sprintf("%d", clusterId), fmt.Sprintf("%d", item)}, delimiter)
		value, err := si.Get(key)
		if err != nil || value == "" {
			return false, err
		}
		var shardTableData *metapb.ShardTopology
		err = proto.Unmarshal([]byte(value), shardTableData)
		if err != nil {
			return false, etcdutil.ErrEtcdKVGet.WithCause(err)
		}
		shardTableInfo = append(shardTableInfo, shardTableData)
	}
	return true, nil
}

func (si *StorageImpl) PutShardTopologies(clusterId uint32, shardId []uint32, shardTableInfo []*metapb.ShardTopology) error {
	for index, item := range shardId {
		key := strings.Join([]string{fmt.Sprintf("%d", clusterId), fmt.Sprintf("%d", item)}, delimiter)
		value, err := proto.Marshal(shardTableInfo[index])
		if err != nil {
			return etcdutil.ErrEtcdKVGet.WithCause(err)
		}
		err = si.Put(key, string(value))
		if err != nil {
			return err
		}
	}
	return nil
}

// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package etcdutil

import "github.com/CeresDB/ceresmeta/pkg/coderr"

var (
	ErrEtcdKVGet         = coderr.NewCodeError(coderr.Internal, "etcd KV get failed")
	ErrEtcdKVGetResponse = coderr.NewCodeError(coderr.Internal, "etcd invalid get value response must only one")
	ErrEtcdKVPut         = coderr.NewCodeError(coderr.Internal, "etcd KV put failed")
	ErrEtcdKVDelete      = coderr.NewCodeError(coderr.Internal, "etcd KV delete failed")
)

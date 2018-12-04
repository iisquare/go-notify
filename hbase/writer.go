package hbase

import (
	"context"
	"github.com/tsuna/gohbase"
	"github.com/tsuna/gohbase/hrpc"
)

type Writer struct {
	client   gohbase.Client
	ZkQuorum string
	ZkRoot   string
}

func (this *Writer) Connect() error  {
	options := gohbase.ZookeeperRoot(this.ZkRoot)
	this.client = gohbase.NewClient(this.ZkQuorum, options)
	return nil
}

func (this *Writer) DisConnection(inInterrupt bool)  {
	if !inInterrupt && this.client != nil {
		this.client.Close()
		this.client = nil
	}
}

func (this *Writer) Put(table string, key string, values map[string]map[string][]byte) (*hrpc.Result, error)  {
	putRequest, err := hrpc.NewPutStr(context.Background(), table, key, values)
	if err != nil {
		return nil, err
	}
	return this.client.Put(putRequest)
}

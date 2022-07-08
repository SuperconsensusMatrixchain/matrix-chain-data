package chain_server

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/xuperchain/xuperchain/service/pb"
	"io"
	"log"
)

// 监听器
type Watcher struct {
	FilteredBlockChan <-chan *pb.InternalBlock
}

// 监听链上的数据
// WatchBlockEvent new watcher for block event.
func (c *ChainClient) WatchBlockEvent(ctx context.Context, bcname string) (*Watcher, error) {
	// 创建监听器
	watcher := &Watcher{}
	// 区块过滤条件
	filter := &pb.BlockFilter{
		Bcname: bcname,
	}
	buf, _ := proto.Marshal(filter)
	request := &pb.SubscribeRequest{
		Type:   pb.SubscribeType_BLOCK,
		Filter: buf,
	}

	// 订阅时间
	//xclient := pb.NewEventServiceClient(conn)
	stream, err := c.esc.Subscribe(context.TODO(), request)
	if err != nil {
		return nil, err
	}

	// 创建管道，用于存放监听到的区块数据
	// 管道大小 （3秒一个快）
	filteredBlockChan := make(chan *pb.InternalBlock, 5)
	watcher.FilteredBlockChan = filteredBlockChan

	go func() {
		defer func() {
			close(filteredBlockChan)
			if err := stream.CloseSend(); err != nil {
				log.Printf("Unregister block event failed, close stream error: %v", err)
			} else {
				log.Printf("Unregister block event success...")
			}
		}()
		for {
			select {
			case <-ctx.Done():
				log.Println("[watch goroutine]---stop watch", bcname)
				return
			default:
				event, err := stream.Recv()
				if err == io.EOF {
					return
				}
				if err != nil {
					log.Printf("Get block event err: %v, %s", err, bcname)
					return
				}
				var block pb.InternalBlock
				err = proto.Unmarshal(event.Payload, &block)
				if err != nil {
					log.Printf("Get block event err: %v, %s", err, bcname)
					return
				}
				//if &block == nil {
				//	continue
				//}
				filteredBlockChan <- &block
			}
		}
	}()
	return watcher, nil
}

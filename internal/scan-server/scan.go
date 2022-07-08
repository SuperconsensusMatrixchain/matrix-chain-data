package scan_server

import (
	"context"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"matrixchaindata/global"
	chain_server "matrixchaindata/internal/chain-server"
	"matrixchaindata/pkg/utils"
	"sync"
)

var (
	// 协程池的容量
	gosize = 20
)

// 扫描器
type Scanner struct {
	// 节点
	Node string
	// 链名
	Bcname string
	// 链客户端
	ChainClient *chain_server.ChainClient
	// 区块数据管道
	BlockChan chan *utils.InternalBlock
	// 解析器
	Parser *Parser
	// 退出方式，使用ctx
	Cannel context.CancelFunc
	// 运行状态
	IsRunning bool
}

// 创建扫描器
func NewScanner(node, bcname string) (*Scanner, error) {
	// 创建链客户端连接
	client, err := chain_server.NewChainClien(node)
	if err != nil {
		return nil, fmt.Errorf("creat chain client fail")
	}

	// 创建解析器
	parser := NewParser(global.GloMongodbClient, node, bcname)

	return &Scanner{
		Node:        node,
		Bcname:      bcname,
		ChainClient: client,
		Parser:      parser,
	}, nil
}

// 停止扫描工作
// 关闭工作是这样的，连接不为空，则发送退出信号
// goroutin 接收到信息，则关闭grpc连接，在退出
// 监听数据很好控制
func (s *Scanner) Stop() {
	if s.Cannel != nil {
		s.Cannel()
	}
}

// 启动扫描工作
func (s *Scanner) Start() error {
	// 创建一个接收区块的管道
	receiveBlockChan := make(chan *utils.InternalBlock, 10)

	// ctx
	ctx, cannel := context.WithCancel(context.Background())

	// 处理订阅的区块
	err := s.GetBlockFromSubscribe(ctx, receiveBlockChan)
	if err != nil {
		return err
	}

	// 扫描数据库中缺少的区块 or 交易
	err = s.GetLackBlock(ctx, receiveBlockChan)
	if err != nil {
		return err
	}

	// 启动解析
	_ = s.Parser.Start(ctx, receiveBlockChan)

	// 设置一下运行状态
	s.IsRunning = true
	s.Cannel = cannel

	return nil
}

// GetBlockFromSubscribe 处理订阅到的区块数据
func (s *Scanner) GetBlockFromSubscribe(ctx context.Context, blockchan chan<- *utils.InternalBlock) error {
	//监听数据
	watcher, err := s.ChainClient.WatchBlockEvent(ctx, s.Bcname)
	if err != nil {
		return err
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case block, ok := <-watcher.FilteredBlockChan:
				if ok {
					blockchan <- utils.FromInternalBlockPB(block)
				}
			}
		}
	}()
	return nil
}

// GetLackBlock  获取表中缺少的区块数据
func (s *Scanner) GetLackBlock(ctx context.Context, blockchan chan<- *utils.InternalBlock) error {
	if blockchan == nil {
		return errors.New(" blockchan is nil")
	}
	go func() {
		// 这里开启协程池
		defer ants.Release()
		wg := sync.WaitGroup{}
		p, _ := ants.NewPoolWithFunc(gosize, func(i interface{}) {
			func(height int64) {
				iblock, err := s.ChainClient.GetBlockByHeight(s.Bcname, height)
				if err != nil {
					log.Printf("get block by height failed,bcname:%s, height: %d, error: %s", s.Bcname, height, err)
					return
				}
				blockchan <- utils.FromInternalBlockPB(iblock)
			}(i.(int64))
			wg.Done()
		})
		defer p.Release()

		heightChan, err := s.GetLackHeights(ctx)
		if err != nil {
			log.Println("get lack height error", err)
			return
		}
		for height := range heightChan {
			wg.Add(1)
			_ = p.Invoke(height)
		}
		log.Println("get blocks finished", s.Node, s.Bcname)
		wg.Wait()
		log.Println("quit lack block gortution")
	}()
	return nil
}

// ------------------------------
// 改进: 找到一个去链上获取，利用管道的特性
// 异步执行
// 使用context 改进
// 父gorution可以控制子gorutine结束, 子goroutin可以通知父goroutin完成任务
// ------------------------------
func (s *Scanner) GetLackHeights(ctx context.Context) (<-chan int64, error) {
	// 最新的区块高度
	_, H, err := s.ChainClient.GetUtxoTotalAndTrunkHeight(s.Bcname)
	if err != nil {
		return nil, err
	}
	log.Println("start get lack blocks", s.Node, s.Bcname)
	// 获取区块集合
	blockCol := s.Parser.MongoClient.Database.Collection(utils.BlockCol(s.Node, s.Bcname))
	//获取数据库中最后的区块高度
	limit := int64(0)
	var heights []int64

	cursor, err := blockCol.Find(nil, bson.M{}, &options.FindOptions{
		Projection: bson.M{"_id": 1},
		Sort:       bson.M{"_id": 1},
		Limit:      &limit,
	})

	if err != nil && err != mongo.ErrNoDocuments {
		return nil, err
	}
	var reply bson.A
	if cursor != nil {
		err = cursor.All(nil, &reply)
	}

	// 需要遍历数据的长度
	length := len(reply)
	//获取需要遍历的区块高度
	heights = make([]int64, length)
	for i, v := range reply {
		heights[i] = v.(bson.D).Map()["_id"].(int64)
	}

	// 处理，找出缺少区块的索引
	heightChan := make(chan int64, 20)
	if length == 0 {
		//第一次同步数据
		go func() {
			log.Println("get height by 1", s.Bcname)
			var i int64 = 1
			for {
				select {
				case <-ctx.Done():
					// 接收到上一级的结束任务通知
					close(heightChan)
					return
				default:
					heightChan <- i
					i++
					if i >= H {
						log.Println("遍历完成，高度获取完成,i , H", i, H)
						// 完成结束，通知父级goroutin
						close(heightChan)
						return
					}
				}
			}
		}()
	} else {
		go func() {
			log.Println("get height by 2", s.Bcname)
			var i int64 = 1
			for {
				select {
				case <-ctx.Done():
					log.Println("exit heightchan", s.Node, s.Bcname)
					//接收到上一级的结束任务通知
					close(heightChan)
					return
				default:
					if SearchInt64(heights, i) == -1 {
						heightChan <- i
					}
					i++
					if i >= H {
						// 完成结束，通知父级goroutin
						log.Println("遍历完成，高度获取完成,i , H", i, H)
						close(heightChan)
						return
					}
				}
			}
		}()
	}
	return heightChan, nil
}

// x 在 a 中的索引
func SearchInt64(a []int64, x int64) int64 {
	var index int64 = -1
	i, j := int64(0), int64(len(a))
	for i < j {
		// 中间
		middle := int64(uint64(i+j) >> 1)
		if a[middle] > x {
			j = middle
		} else if a[middle] < x {
			i = middle + 1
		} else {
			return middle
		}
	}
	return index
}

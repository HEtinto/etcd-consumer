package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

const (
	leaseTTL = 10 // 10秒过期
)

// 故障恢复设计
// 当消费者执行到一半重启时
// 重新启动时, 从 etcd 中获取未处理的消息, 并重新消费

// 负载均衡设计
// 消息队列 + 分片负载均衡
// 特定的 consumer 只消费特定的分片
// consumer0 -> 消费分片 0
// consumer1 -> 消费分片 1
// consumer2 -> 消费分片 2

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: go run main.go <etcd-address>")
	}

	endpoints := []string{os.Args[1]}
	logger := log.New(os.Stdout, "[go-consumer] ", log.LstdFlags)
	logger.Printf("连接 etcd: %v", endpoints)

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		logger.Fatalf("连接失败: %v", err)
	}
	defer cli.Close()

	watchPrefix := "/messages/"
	watchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watchChan := cli.Watch(watchCtx, watchPrefix, clientv3.WithPrefix())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	logger.Println("✅ 并行消费者已启动，等待消息...")

	for {
		select {
		case <-sigChan:
			logger.Println("🛑 退出")
			return

		case wr := <-watchChan:
			for _, ev := range wr.Events {
				if ev.Type != clientv3.EventTypePut {
					continue
				}

				key := string(ev.Kv.Key)
				value := string(ev.Kv.Value)

				// 只处理未被占用的消息
				if ev.Kv.Lease != 0 {
					continue
				}

				go processMessageParallel(cli, logger, key, value)
			}
		}
	}
}

func processMessageParallel(cli *clientv3.Client, logger *log.Logger, key, value string) {
	// 1. 创建租约
	leaseResp, err := cli.Grant(context.TODO(), leaseTTL)
	if err != nil {
		return
	}

	// 2. 事务原子抢占: 如果 key 没有租约, 就绑定租约
	cmp := clientv3.Compare(clientv3.LeaseValue(key), "=", 0)
	putOp := clientv3.OpPut(key, value, clientv3.WithLease(leaseResp.ID))
	txnResp, err := cli.Txn(context.TODO()).If(cmp).Then(putOp).Commit()

	if err != nil || !txnResp.Succeeded {
		// 没抢到, 直接退出
		return
	}

	// 3. 抢到了, 开始消费
	logger.Printf("[开始消费] key: %s", key)

	// 自动续期
	keepAlive, err := cli.KeepAlive(context.Background(), leaseResp.ID)
	if err == nil {
		go func() {
			for range keepAlive {
			}
		}()
	}

	// 模拟业务
	time.Sleep(2 * time.Second)

	// 4. 消费完成, 删除消息
	_, err = cli.Delete(context.TODO(), key)
	if err == nil {
		logger.Printf("[消费完成] key: %s", key)
	}
}

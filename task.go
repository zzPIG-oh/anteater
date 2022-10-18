package task

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"anteater/util"

	"github.com/go-redis/redis/v8"
)

var (
	consumeGap = 1500 * time.Millisecond
	watchGap   = 20 * time.Second
)

type TaskQueue interface {

	// -
	//	pull 从redis的list内拉取消息
	//	加分布式锁
	Pull()

	// -
	//	ack消费完每个节点自行获取下一次拉取的权限
	//	锁释放
	// Ack()
}

type taskQueue struct {
	ctx context.Context

	// -
	//	queueC:协程
	queueC map[uint32]chan struct{}

	// -
	//	queue:协程
	queue map[string]uint32

	// -
	//	ctl:redis客户端
	ctl *redis.Client

	// -
	//	handle 处理函数
	handle func(context.Context, string) error
}

// -
//	SetConsumeGap
//	建议消费间隔在1-2秒
func SetConsumeGap(gap time.Duration) {
	consumeGap = gap
}

// -
//	SetWatchGap
//	建议是消费间隔的5-10倍
func SetWatchGap(gap time.Duration) {
	watchGap = gap
}

// -
//	NewTaskQueue:声明任务队列
//	@param:	handle作为有任务后的统一调用入口
func NewTaskQueue(ctx context.Context, ctl *redis.Client, handle func(context.Context, string) error) (TaskQueue, error) {

	tq := &taskQueue{
		ctx:    ctx,
		ctl:    ctl,
		handle: handle,
		queueC: make(map[uint32]chan struct{}, 1),
		queue:  map[string]uint32{},
	}

	waitGoNum, err := tq.ctl.Keys(tq.ctx, util.Output(0, 0)).Result()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(waitGoNum); i++ {
		tag := tq.rand()
		tq.queueC[tag] = make(chan struct{}, 1)
		tq.queue[waitGoNum[i]] = tag
	}

	go tq.watch()

	return tq, nil
}

// -
//	rand
//	随机队列、channle标识
func (tq *taskQueue) rand() uint32 {

	rand.Seed(time.Now().UnixMilli())

	tag := rand.Uint32()

	if _, ok := tq.queueC[tag]; ok {
		tq.rand()
	}

	return tag
}

// -
//	watch
//	监听keys的变化
func (tq *taskQueue) watch() {

	for range time.Tick(watchGap) {

		waitGoNum, err := tq.ctl.Keys(tq.ctx, util.Output(0, 0)).Result()

		if err != nil {
			fmt.Println(err.Error())
			continue
		}

		waitGoMap := map[string]struct{}{}

		for _, v := range waitGoNum {

			waitGoMap[v] = struct{}{}

		}

		var ifRefresh bool

		for k, v := range tq.queue {

			if _, ok := waitGoMap[k]; !ok {

				if tq.queueC[v] != nil {
					close(tq.queueC[v])
				}

				delete(tq.queueC, tq.queue[k])
				delete(tq.queue, k)

				ifRefresh = true

			}

		}

		for k := range waitGoMap {

			if _, ok := tq.queue[k]; !ok {

				tag := tq.rand()

				tq.queue[k] = tag
				tq.queueC[tag] = make(chan struct{}, 1)

				ifRefresh = true
			}

		}

		if ifRefresh {
			tq.Pull()
		}

	}

}

func (tq *taskQueue) Pull() {

	for k, v := range tq.queue {

		go func(key string, sign chan struct{}) {

			var (
				run  bool
				tick = time.NewTicker(consumeGap)
			)

			for {

				select {
				case <-tick.C:

					task, err := tq.ctl.RPop(tq.ctx, key).Result()
					if err != nil && err.Error() != "redis: nil" {
						fmt.Println("get task error,detail", err.Error())
					}

					run = true
					// handle task
					err = tq.handle(tq.ctx, task)
					if err != nil {
						fmt.Println("handle.task error,detail", err.Error())
					}
					run = false

				case <-sign:

					start := time.Now()

					// 监听到退出又有任务在执行时等候三秒
					if run {
						time.Sleep(3 * time.Second)
					}

					fmt.Println("channle close,key", key, "speed", time.Since(start))

					return

				}

			}

		}(k, tq.queueC[v])

	}

}

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
	//	Pull 从redis的list内拉取消息
	//	加分布式锁
	Pull()

	// -
	//	ack消费完每个节点自行获取下一次拉取的权限
	//	锁释放
	// Ack()

	// -
	//	Stop支持优雅退出
	//	当主进程有退出信号后,可调用此方法暂停任务的拉取
	Stop()
}

type taskQueue struct {
	ctx context.Context

	// -
	// //	queueC:任务队列所依赖的通信channle
	// queueC map[uint32]chan struct{}

	// // -
	// //	queueT:任务队列的标识
	// queueT map[uint32]bool

	// -
	//	queue:协程
	queue map[string]*content

	// -
	//	ctl:redis客户端
	ctl *redis.Client

	// -
	//	handle 处理函数
	handle func(context.Context, string) error

	// -
	//	是否停止
	ifStop bool
}

type content struct {
	tag    uint32
	signl  chan struct{}
	ifNew  bool
	tagMap map[uint32]struct{}
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
	}

	waitGoNum, err := tq.ctl.Keys(tq.ctx, util.Output()).Result()
	if err != nil {
		return nil, err
	}

	for i := 0; i < len(waitGoNum); i++ {
		tq.queue[waitGoNum[i]] = &content{
			signl: make(chan struct{}, 1),
			ifNew: true,
		}

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

	return tag
}

// -
//	watch
//	监听keys的变化
func (tq *taskQueue) watch() {

	for range time.Tick(watchGap) {

		if tq.ifStop {
			continue
		}

		waitGoNum, err := tq.ctl.Keys(tq.ctx, util.Output()).Result()

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

			if v == nil {
				continue
			}

			// 刷新旧任务队列标识值
			tq.queue[k].ifNew = false
			if _, ok := waitGoMap[k]; !ok {
				close(tq.queue[k].signl)
				delete(tq.queue, k)
				ifRefresh = true
			}

		}

		for k := range waitGoMap {

			if _, ok := tq.queue[k]; !ok {

				tq.queue[k] = &content{
					ifNew: true,
					signl: make(chan struct{}, 1),
				}

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

		if !v.ifNew {
			continue
		}

		go func(key string, signal chan struct{}) {

			var (
				run  bool
				tick = time.NewTicker(consumeGap)
			)

			for {

				select {
				case <-tick.C:

					if tq.ifStop {
						continue
					}

					task, err := tq.ctl.RPop(tq.ctx, key).Result()
					if err != nil && err.Error() != "redis: nil" {
						fmt.Println("get task error,detail", err.Error())
					}

					run = true

					// handle task
					if tq.handle(tq.ctx, task) != nil {
						fmt.Println("handle.task error,detail", err.Error())
					}

					if tq.ctl.LPush(tq.ctx, key, task).Err() != nil {
						fmt.Println("handle.task.error,attempt.push.error:", err.Error(), "\ntask.content:", task)
					}

					run = false

				case <-signal:

					start := time.Now()

					// 监听到退出又有任务在执行时等候三秒
					if run {
						time.Sleep(3 * time.Second)
					}

					fmt.Println("channle close,key", key, "speed", time.Since(start))

					return

				}

			}

		}(k, tq.queue[k].signl)

	}

}

func (tq *taskQueue) Stop() {
	tq.ifStop = true
}

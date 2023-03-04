package timewheel

import (
	"container/list"
	"go-redis/utils/logger"
	"time"
)

// 代表一个定时任务
type task struct {
	key    string        // task的标识
	job    func()        // task实际要执行的函数
	delay  time.Duration // 延迟执行的时间
	circle int           // 延迟的轮数
}

// 用于定位一个task
type taskLoc struct {
	idx int           // task所处的list
	ele *list.Element // task所位于的Element
}

type TimeWheel struct {
	ticker   *time.Ticker
	interval time.Duration // 每格的时间跨度
	currSlot int           // 当前指向的时间格

	tasks     map[string]*taskLoc // 记录task在时间轮中所处的位置
	slots     []*list.List        // 时间格数组，实际存放task的list
	slotCount int                 // slot个数

	addCh    chan *task  // 添加任务
	removeCh chan string // 移除任务
	closeCh  chan bool   // 终止信号
}

func MakeTimeWheel(interval time.Duration, slotNum int) *TimeWheel {
	if interval <= 0 || slotNum <= 0 {
		panic("illegal interval or slotNum")
	}
	tw := &TimeWheel{
		interval:  interval,
		currSlot:  0,
		tasks:     make(map[string]*taskLoc),
		slots:     make([]*list.List, slotNum),
		slotCount: slotNum,
		addCh:     make(chan *task),
		removeCh:  make(chan string),
		closeCh:   make(chan bool),
	}
	for i := 0; i < tw.slotCount; i++ {
		tw.slots[i] = list.New()
	}
	return tw
}

func (tw *TimeWheel) AddTask(delay time.Duration, key string, job func()) {
	if delay < 0 {
		return
	}
	t := &task{
		key:   key,
		job:   job,
		delay: delay,
	}
	tw.addCh <- t
}

func (tw *TimeWheel) RemoveTask(key string) {
	tw.removeCh <- key
}

func (tw *TimeWheel) Run() {
	tw.ticker = time.NewTicker(tw.interval)
	go func() {
		for {
			select {
			case <-tw.ticker.C:
				tw.scanSlot() // 每次tick扫描一个slot
			case t := <-tw.addCh:
				tw.addTask(t) // 添加任务
			case key := <-tw.removeCh:
				tw.removeTask(key) // 移除任务
			case <-tw.closeCh:
				tw.ticker.Stop() // 停止时间轮
				return
			}
		}
	}()
}

func (tw *TimeWheel) Close() {
	tw.closeCh <- true
}

func (tw *TimeWheel) scanSlot() {
	slot := tw.slots[tw.currSlot]
	go func() {
		ele := slot.Front()
		for ele != nil {
			t := ele.Value.(*task)
			// 检查circle是否已经为0
			if t.circle > 0 {
				t.circle--
				ele = ele.Next()
				continue
			}
			// 执行task
			go func() {
				// 异常处理
				defer func() {
					if err := recover(); err != nil {
						logger.Error(err)
					}
				}()
				t.job()
			}()
			// 移除当前task
			next := ele.Next()
			slot.Remove(ele)
			delete(tw.tasks, t.key)
			ele = next
		}
	}()
	tw.currSlot = (tw.currSlot + 1) % tw.slotCount // 更新当前slot
}

func (tw *TimeWheel) addTask(t *task) {
	delay := int(t.delay)
	interval := int(tw.interval)
	t.circle = delay / interval / tw.slotCount           // 设置task的circle
	idx := (delay/interval + tw.currSlot) % tw.slotCount // 计算该task应该存放的时间格
	// 加入对应的时间格
	ele := tw.slots[idx].PushBack(t)
	loc := &taskLoc{
		idx: idx,
		ele: ele,
	}
	// 若该task已经存在，先移除该task
	key := t.key
	_, ok := tw.tasks[key]
	if ok {
		tw.removeTask(key)
	}
	tw.tasks[key] = loc
}

func (tw *TimeWheel) removeTask(key string) {
	loc, ok := tw.tasks[key]
	if !ok {
		return
	}
	tw.slots[loc.idx].Remove(loc.ele) // 从对应的时间格移除
	delete(tw.tasks, key)             // 从tasks中移除
}

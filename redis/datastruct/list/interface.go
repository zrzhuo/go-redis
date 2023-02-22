package list

// Condition 判断元素是否满足条件
type Condition[T any] func(val T) bool

// Consumer 消费元素，并判断是否继续
type Consumer[T any] func(i int, val T) bool

type List[T any] interface {
	Len() int
	Add(val T)
	Get(idx int) T
	Set(idx int, val T)
	Insert(idx int, val T)
	Remove(idx int) T                                // 移除指定下标的元素，并返回其值
	RemoveAll(condition Condition[T]) int            //  移除所有满足条件的元素，并返回其个数
	RemoveLeft(condition Condition[T], num int) int  // 从左到右移除指定数量的元素，并返回实际移除的个数
	RemoveRight(condition Condition[T], num int) int // 从右到左移除指定数量的元素，并返回实际移除的个数
	Contains(condition Condition[T]) bool            // 判断列表中是否具有满足条件的元素
	Range(start int, stop int) []T                   // 获取指定区间[start, stop)内的所有元素
	ForEach(consumer Consumer[T])
}

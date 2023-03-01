package set

type Set[T comparable] interface {
	Len() int
	Add(val T) int    // 添加成员，并返回实际添加的个数
	Remove(val T) int // 移除成员，并返回实际移除的个数
	Contain(val T) bool
	Members() []T
	RandomMembers(num int) []T         // 随机返回指定数量的member，且member可以重复
	RandomDistinctMembers(num int) []T // 随机返回指定数量的member，且member不可以重复
	ForEach(consumer Consumer[T])
	Inter(ano Set[T]) Set[T] // 求交集
	Diff(ano Set[T]) Set[T]  // 求差集
	Union(ano Set[T]) Set[T] // 求并集
}

type Consumer[T comparable] func(member T) bool

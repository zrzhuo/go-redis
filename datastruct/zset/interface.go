package zset

type ZSet[T comparable] interface {
	Len() int
	Add(member T, score float64) int
	Get(member T) (float64, bool)
	Remove(member T) bool
	GetRank(member T, desc bool) int
	ForEach(start int, stop int, desc bool, consumer Consumer[T])
}

type Consumer[T comparable] func(member T, score float64) bool

type Compare[T comparable] func(T, T) int // 用于比较的函数

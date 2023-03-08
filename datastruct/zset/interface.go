package zset

type ZSet[T comparable] interface {
	Len() int
	RangeLen(min float64, max float64) int
	Add(member T, score float64) int
	Contains(member T) bool
	GetScore(member T) (float64, bool)
	GetRank(member T, desc bool) (int, bool)
	Remove(member T) int
	RemoveRangeByScore(min float64, max float64) int
	RemoveRangeByRank(start int, stop int) int
	ForEach(start int, stop int, desc bool, consumer Consumer[T])
}

type Consumer[T comparable] func(member T, score float64) bool

type Compare[T comparable] func(T, T) int // 用于比较的函数

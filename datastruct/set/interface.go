package set

type Set[T comparable] interface {
	Len() int
	Add(val T) int
	Remove(val T) int
	Contain(val T) bool
	Members() []T
	RandomMembers(num int) []T
	RandomDistinctMembers(num int) []T
	ForEach(consumer Consumer[T])
	Intersect(ano Set[T]) Set[T]
	Diff(ano Set[T]) Set[T]
	Union(ano Set[T]) Set[T]
}

type Consumer[T comparable] func(member T) bool

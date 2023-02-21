package set

import (
	"go-redis/database/datastruct/dict"
	"go-redis/database/datastruct/list"
)

type SortedSet[T comparable] struct {
	dict     dict.Dict[T, int64]
	skiplist *list.SkipList[T]
}

func MakeSortedSet[T comparable](comp list.Compare[T]) *SortedSet[T] {
	return &SortedSet[T]{
		dict:     dict.MakeSimpleDict[T, int64](),
		skiplist: list.MakeSkiplist[T](comp),
	}
}

package zset

import (
	"fmt"
	"go-redis/database/datastruct/dict"
)

type Compare[T comparable] func(T, T) int // 用于比较的函数

type Consumer[T comparable] func(member T, score float64) bool

type SortedSet[T comparable] struct {
	dict     dict.Dict[T, float64]
	skiplist *SkipList[T]
}

func MakeSortedSet[T comparable](comp Compare[T]) *SortedSet[T] {
	return &SortedSet[T]{
		dict:     dict.MakeSimpleDict[T, float64](),
		skiplist: MakeSkiplist[T](comp),
	}
}

func (set *SortedSet[T]) Len() int {
	if set == nil {
		panic("this SortedSet is nil.")
	}
	return set.dict.Len()
}

func (set *SortedSet[T]) Add(member T, score float64) bool {
	if set == nil {
		panic("this SortedSet is nil.")
	}
	oldScore, existed := set.dict.Get(member)
	set.dict.Put(member, score)
	if existed {
		if score != oldScore {
			set.skiplist.Remove(member, oldScore)
			set.skiplist.Insert(member, score)
		}
		return false
	}
	set.skiplist.Insert(member, score)
	return true
}

func (set *SortedSet[T]) Get(member T) (float64, bool) {
	if set == nil {
		panic("this SortedSet is nil.")
	}
	score, existed := set.dict.Get(member)
	if !existed {
		return -1, false
	}
	return score, true
}

func (set *SortedSet[T]) Remove(member T) bool {
	if set == nil {
		panic("this SortedSet is nil.")
	}
	score, exited := set.dict.Get(member)
	if !exited {
		return false
	}
	set.dict.Remove(member)
	set.skiplist.Remove(member, score)
	return true
}

func (set *SortedSet[T]) GetRank(member T, desc bool) int {
	if set == nil {
		panic("this SortedSet is nil.")
	}
	score, existed := set.dict.Get(member)
	if !existed {
		return -1
	}
	rank := set.skiplist.GetRank(member, score)
	if desc {
		return set.Len() - 1 - rank
	}
	return rank
}

func (set *SortedSet[T]) ForEach(start int, stop int, desc bool, consumer Consumer[T]) {
	if set == nil {
		panic("this SortedSet is nil.")
	}
	size := set.Len()
	if start < 0 || start >= size {
		panic(fmt.Sprintf("the start index %d out of bound", start))
	}
	if stop < start || start > size {
		panic(fmt.Sprintf("the stop index %d out of bound", stop))
	}
	var node *skipNode[T]
	if !desc {
		node = set.skiplist.getNodeByRank(start)
	} else {
		node = set.skiplist.getNodeByRank(size - 1 - start)
	}
	for i := 0; i < stop-start; i++ {
		if consumer(node.Member, node.Score) {
			if !desc {
				node = node.levels[0].next
			} else {
				node = node.prev
			}
		} else {
			break
		}
	}
}

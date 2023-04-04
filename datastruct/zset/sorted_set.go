package zset

import (
	"fmt"
	Dict "go-redis/datastruct/dict"
)

type SortedSet[T comparable] struct {
	dict     Dict.Dict[T, float64] // 用于快速查找
	skiplist *SkipList[T]          // 用于保持有序性的跳表
}

func MakeSortedSet[T comparable](comp Compare[T]) ZSet[T] {
	return &SortedSet[T]{
		dict:     Dict.NewSimpleDict[T, float64](),
		skiplist: NewSkiplist[T](comp),
	}
}

func (set *SortedSet[T]) Len() int {
	if set == nil {
		panic("this SortedSet is nil")
	}
	return set.dict.Len()
}

func (set *SortedSet[T]) RangeLen(min float64, max float64) int {
	if set == nil {
		panic("this SortedSet is nil")
	}
	return set.skiplist.CountRange(min, max)
}

func (set *SortedSet[T]) Add(member T, score float64) int {
	if set == nil {
		panic("this SortedSet is nil")
	}
	oldScore, existed := set.dict.Get(member)
	set.dict.Put(member, score)
	if existed {
		if score != oldScore {
			set.skiplist.Remove(member, oldScore)
			set.skiplist.Insert(member, score)
		}
		return 0
	}
	set.skiplist.Insert(member, score)
	return 1
}

func (set *SortedSet[T]) Remove(member T) int {
	if set == nil {
		panic("this SortedSet is nil")
	}
	score, exited := set.dict.Get(member)
	if !exited {
		return 0
	}
	set.dict.Remove(member)
	set.skiplist.Remove(member, score)
	return 1
}

func (set *SortedSet[T]) RemoveRangeByScore(min float64, max float64) int {
	if set == nil {
		panic("this SortedSet is nil")
	}
	nodes := set.skiplist.RemoveRangeByScore(min, max, set.Len())
	for _, node := range nodes {
		set.dict.Remove(node.Obj)
	}
	return len(nodes)
}

func (set *SortedSet[T]) RemoveRangeByRank(start int, stop int) int {
	if set == nil {
		panic("this SortedSet is nil")
	}
	nodes := set.skiplist.RemoveRangeByRank(start, stop)
	for _, node := range nodes {
		set.dict.Remove(node.Obj)
	}
	return len(nodes)
}

func (set *SortedSet[T]) Contains(member T) bool {
	if set == nil {
		panic("this SortedSet is nil")
	}
	_, existed := set.dict.Get(member)
	if !existed {
		return false
	}
	return true
}

func (set *SortedSet[T]) GetScore(member T) (float64, bool) {
	if set == nil {
		panic("this SortedSet is nil")
	}
	score, existed := set.dict.Get(member)
	if !existed {
		return -1, false
	}
	return score, true
}

func (set *SortedSet[T]) GetRank(member T, desc bool) (int, bool) {
	if set == nil {
		panic("this SortedSet is nil")
	}
	score, existed := set.dict.Get(member)
	if !existed {
		return -1, false
	}
	rank := set.skiplist.GetRank(member, score)
	if desc {
		return set.Len() - 1 - rank, true
	}
	return rank, true
}

func (set *SortedSet[T]) ForEach(startRank int, stopRank int, desc bool, consumer Consumer[T]) {
	if set == nil {
		panic("this SortedSet is nil")
	}
	size := set.Len()
	if startRank < 0 || startRank >= size {
		panic(fmt.Sprintf("the startRank index %d out of bound", startRank))
	}
	if stopRank < startRank || startRank > size {
		panic(fmt.Sprintf("the stopRank index %d out of bound", stopRank))
	}
	var node *SkipNode[T]
	if !desc {
		node = set.skiplist.GetNodeByRank(startRank)
	} else {
		node = set.skiplist.GetNodeByRank(size - 1 - startRank)
	}
	for i := 0; i < stopRank-startRank; i++ {
		if consumer(node.Obj, node.Score) {
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

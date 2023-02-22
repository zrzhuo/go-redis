package set

import (
	Dict "go-redis/datastruct/dict"
)

type Consumer[T comparable] func(member T) bool

// Set 对SimpleDict[K,V]的再次包装
type Set[T comparable] struct {
	dict Dict.Dict[T, any]
}

func MakeSimpleSet[T comparable](members ...T) *Set[T] {
	set := &Set[T]{
		dict: Dict.MakeSimpleDict[T, any](),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (set *Set[T]) Len() int {
	return set.dict.Len()
}

func (set *Set[T]) Add(val T) int {
	return set.dict.Put(val, nil)
}

func (set *Set[T]) Remove(val T) int {
	return set.dict.Remove(val)
}

func (set *Set[T]) Contain(val T) bool {
	_, exists := set.dict.Get(val)
	return exists
}

func (set *Set[T]) ForEach(consumer Consumer[T]) {
	set.dict.ForEach(func(key T, val any) bool {
		return consumer(key)
	})
}

func (set *Set[T]) Members() []T {
	return set.dict.Keys()
}

// RandomMembers 随机返回指定数量的member，且member可以重复
func (set *Set[T]) RandomMembers(num int) []T {
	return set.dict.RandomKeys(num)
}

// RandomDistinctMembers 随机返回指定数量的member，且member不可以重复
func (set *Set[T]) RandomDistinctMembers(num int) []T {
	return set.dict.RandomDistinctKeys(num)
}

// Intersect 求交集
func (set *Set[T]) Intersect(ano *Set[T]) *Set[T] {
	if set == nil {
		panic("set is nil")
	}
	result := MakeSimpleSet[T]()
	set.ForEach(func(member T) bool {
		if ano.Contain(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// Diff 求差集
func (set *Set[T]) Diff(ano *Set[T]) *Set[T] {
	if set == nil {
		panic("set is nil")
	}
	result := MakeSimpleSet[T]()
	set.ForEach(func(member T) bool {
		if !ano.Contain(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

// Union 求并集
func (set *Set[T]) Union(ano *Set[T]) *Set[T] {
	if set == nil {
		panic("set is nil")
	}
	result := MakeSimpleSet[T]()
	set.ForEach(func(member T) bool {
		result.Add(member)
		return true
	})
	ano.ForEach(func(member T) bool {
		result.Add(member)
		return true
	})
	return result
}

package set

import (
	Dict "go-redis/datastruct/dict"
)

// SimpleSet 对SimpleDict[K,V]的再次包装
type SimpleSet[T comparable] struct {
	dict Dict.Dict[T, any]
}

func NewSimpleSet[T comparable](members ...T) Set[T] {
	set := &SimpleSet[T]{
		dict: Dict.NewSimpleDict[T, any](),
	}
	for _, member := range members {
		set.Add(member)
	}
	return set
}

func (set *SimpleSet[T]) Len() int {
	return set.dict.Len()
}

func (set *SimpleSet[T]) Add(val T) int {
	return set.dict.Put(val, nil)
}

func (set *SimpleSet[T]) Remove(val T) int {
	return set.dict.Remove(val)
}

func (set *SimpleSet[T]) Contain(val T) bool {
	_, exists := set.dict.Get(val)
	return exists
}

func (set *SimpleSet[T]) ForEach(consumer Consumer[T]) {
	set.dict.ForEach(func(key T, val any) bool {
		return consumer(key)
	})
}

func (set *SimpleSet[T]) Members() []T {
	return set.dict.Keys()
}

func (set *SimpleSet[T]) RandomMembers(num int) []T {
	return set.dict.RandomKeys(num)
}

func (set *SimpleSet[T]) RandomDistinctMembers(num int) []T {
	return set.dict.RandomDistinctKeys(num)
}

func (set *SimpleSet[T]) Inter(ano Set[T]) Set[T] {
	if set == nil {
		panic("set is nil")
	}
	result := NewSimpleSet[T]()
	set.ForEach(func(member T) bool {
		if ano.Contain(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

func (set *SimpleSet[T]) Diff(ano Set[T]) Set[T] {
	if set == nil {
		panic("set is nil")
	}
	result := NewSimpleSet[T]()
	set.ForEach(func(member T) bool {
		if !ano.Contain(member) {
			result.Add(member)
		}
		return true
	})
	return result
}

func (set *SimpleSet[T]) Union(ano Set[T]) Set[T] {
	if set == nil {
		panic("set is nil")
	}
	result := NewSimpleSet[T]()
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

package dict

import (
	"go-redis/utils/fnv"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type bucket[K comparable, V any] struct {
	m     map[K]V      // 哈希表
	mutex sync.RWMutex // 锁
}

// 随机获取一个key
func (bucket *bucket[K, V]) randomKey() []K {
	if bucket == nil {
		panic("bucket is nil")
	}
	bucket.mutex.RLock()
	defer bucket.mutex.RUnlock()

	for key := range bucket.m {
		return []K{key} // 做一层包装，以配合nil
	}
	return nil
}

type ConcurrentDict[K comparable, V any] struct {
	buckets []*bucket[K, V]
	size    int32
	length  int32
}

func checkNilDict(dict any) {
	if dict == nil {
		panic("dict is nil")
	}
}

func computeCapacity(param int32) (size int32) {
	if param <= 16 {
		return 16 // 最小容量为16
	}
	n := param - 1
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	if n < 0 {
		return math.MaxInt32
	}
	return n + 1
}

func MakeConcurrentDict[K comparable, V any](size int32) *ConcurrentDict[K, V] {
	size = computeCapacity(size)
	buckets := make([]*bucket[K, V], size)
	for i := int32(0); i < size; i++ {
		buckets[i] = &bucket[K, V]{m: make(map[K]V)}
	}
	return &ConcurrentDict[K, V]{
		buckets: buckets,
		size:    size,
		length:  0,
	}
}

// 定位bucket
func (dict *ConcurrentDict[K, V]) spread(hashCode uint32) uint32 {
	checkNilDict(dict)
	tableSize := uint32(len(dict.buckets))
	return (tableSize - 1) & hashCode
}

// 获取指定bucket
func (dict *ConcurrentDict[K, V]) getShard(index uint32) *bucket[K, V] {
	checkNilDict(dict)
	return dict.buckets[index]
}

// 根据key计算其应该存放的bucket
func (dict *ConcurrentDict[K, V]) computeShard(key K) *bucket[K, V] {
	hashCode := fnv.Fnv32(key)
	index := dict.spread(hashCode)
	return dict.getShard(index)
}

func (dict *ConcurrentDict[K, V]) Len() int {
	checkNilDict(dict)
	return int(atomic.LoadInt32(&dict.length))
}

func (dict *ConcurrentDict[K, V]) Get(key K) (val V, existed bool) {
	checkNilDict(dict)
	s := dict.computeShard(key)
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	val, existed = s.m[key]
	return
}

func (dict *ConcurrentDict[K, V]) Put(key K, val V) (result int) {
	checkNilDict(dict)
	s := dict.computeShard(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 0
	}
	atomic.AddInt32(&dict.length, 1)
	s.m[key] = val
	return 1
}

func (dict *ConcurrentDict[K, V]) PutIfAbsent(key K, val V) (result int) {
	checkNilDict(dict)
	s := dict.computeShard(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		return 0
	}
	s.m[key] = val
	atomic.AddInt32(&dict.length, 1)
	return 1
}

func (dict *ConcurrentDict[K, V]) PutIfExists(key K, val V) (result int) {
	checkNilDict(dict)
	s := dict.computeShard(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		s.m[key] = val
		return 1
	}
	return 0
}

func (dict *ConcurrentDict[K, V]) Remove(key K) (result int) {
	checkNilDict(dict)
	s := dict.computeShard(key)
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if _, ok := s.m[key]; ok {
		delete(s.m, key)
		atomic.AddInt32(&dict.length, -1)
		return 1
	}
	return 0
}

func (dict *ConcurrentDict[K, V]) Keys() []K {
	checkNilDict(dict)
	keys := make([]K, dict.Len())
	i := 0
	dict.ForEach(func(key K, val V) bool {
		if i < len(keys) {
			keys[i] = key
			i++
		} else {
			keys = append(keys, key)
		}
		return true
	})
	return keys
}

func (dict *ConcurrentDict[K, V]) RandomKeys(num int) []K {
	checkNilDict(dict)
	size := dict.Len()
	if num >= size {
		return dict.Keys()
	}
	shardCount := len(dict.buckets)
	result := make([]K, num)
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < num; {
		shard := dict.getShard(uint32(nR.Intn(shardCount)))
		if shard == nil {
			continue
		}
		key := shard.randomKey()
		if key != nil {
			result[i] = key[0]
			i++
		}
	}
	return result
}

func (dict *ConcurrentDict[K, V]) RandomDistinctKeys(num int) []K {
	checkNilDict(dict)
	size := dict.Len()
	if num >= size {
		return dict.Keys()
	}
	shardCount := len(dict.buckets)
	result := make(map[K]struct{})
	nR := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(result) < num {
		shardIndex := uint32(nR.Intn(shardCount))
		shard := dict.getShard(shardIndex)
		if shard == nil {
			continue
		}
		key := shard.randomKey()
		if key != nil {
			if _, exists := result[key[0]]; !exists {
				result[key[0]] = struct{}{}
			}
		}
	}
	arr := make([]K, num)
	i := 0
	for k := range result {
		arr[i] = k
		i++
	}
	return arr
}

func (dict *ConcurrentDict[K, V]) ForEach(consumer Consumer[K, V]) {
	checkNilDict(dict)
	for _, s := range dict.buckets {
		s.mutex.RLock()
		// 使用匿名函数是为了 s.mutex.RUnlock() 正常执行
		func() {
			defer s.mutex.RUnlock()
			for key, value := range s.m {
				continues := consumer(key, value)
				if !continues {
					return
				}
			}
		}()
	}
}

func (dict *ConcurrentDict[K, V]) Clear() {
	checkNilDict(dict)
	*dict = *MakeConcurrentDict[K, V](dict.size)
}

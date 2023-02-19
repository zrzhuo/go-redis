package dict

import (
	"go-redis/utils"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type shard[K comparable, V any] struct {
	m     map[K]V      // 哈希表
	mutex sync.RWMutex // 锁
}

// 随机获取一个key
func (shard *shard[K, V]) randomKey() []K {
	if shard == nil {
		panic("shard is nil")
	}
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()

	for key := range shard.m {
		return []K{key} // 做一层包装，以配合nil
	}
	return nil
}

type ConcurrentDict[K comparable, V any] struct {
	table      []*shard[K, V]
	count      int32
	shardCount int32
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

func MakeConcurrent[K comparable, V any](shardCount int32) *ConcurrentDict[K, V] {
	shardCount = computeCapacity(shardCount)
	table := make([]*shard[K, V], shardCount)
	for i := int32(0); i < shardCount; i++ {
		table[i] = &shard[K, V]{m: make(map[K]V)}
	}
	d := &ConcurrentDict[K, V]{
		table:      table,
		count:      0,
		shardCount: shardCount,
	}
	return d
}

// 定位shard
func (dict *ConcurrentDict[K, V]) spread(hashCode uint32) uint32 {
	checkNilDict(dict)
	tableSize := uint32(len(dict.table))
	return (tableSize - 1) & hashCode
}

// 获取指定shard
func (dict *ConcurrentDict[K, V]) getShard(index uint32) *shard[K, V] {
	checkNilDict(dict)
	return dict.table[index]
}

// 根据key计算其应该存放的shard
func (dict *ConcurrentDict[K, V]) computeShard(key K) *shard[K, V] {
	hashCode := utils.Fnv32(key)
	index := dict.spread(hashCode)
	return dict.getShard(index)
}

func (dict *ConcurrentDict[K, V]) Len() int {
	checkNilDict(dict)
	return int(atomic.LoadInt32(&dict.count))
}

func (dict *ConcurrentDict[K, V]) Get(key K) (val V, exists bool) {
	checkNilDict(dict)
	s := dict.computeShard(key)
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	val, exists = s.m[key]
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
	atomic.AddInt32(&dict.count, 1)
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
	atomic.AddInt32(&dict.count, 1)
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
		atomic.AddInt32(&dict.count, -1)
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
	shardCount := len(dict.table)
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
	shardCount := len(dict.table)
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
	for _, s := range dict.table {
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
	*dict = *MakeConcurrent[K, V](dict.shardCount)
}

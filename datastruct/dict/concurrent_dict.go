package dict

import (
	"go-redis/utils/fnv"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// 一个bucket含有一个map和一把锁，访问map前需要获取锁
type bucket[K comparable, V any] struct {
	m    map[K]V      // 哈希表
	lock sync.RWMutex // 锁
}

// 随机获取一个key
func (bucket *bucket[K, V]) randomKey() []K {
	if bucket == nil {
		panic("bucket is nil")
	}
	// 获取读锁
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	for key := range bucket.m {
		return []K{key} // 做一层包装，以配合nil
	}
	return nil // bucket无元素
}

// ConcurrentDict 并发安全的Dict，其中包含若干个bucket，每个bucket都有自己独立的锁
type ConcurrentDict[K comparable, V any] struct {
	buckets   []*bucket[K, V]
	bucketNum int32 // bucket个数
	length    int32 // 包含键值对的个数
}

func NewConcurrentDict[K comparable, V any](num int32) *ConcurrentDict[K, V] {
	bucketNum := computeBucketNum(num) // 计算bucket的合适个数
	buckets := make([]*bucket[K, V], bucketNum)
	for i := int32(0); i < bucketNum; i++ {
		buckets[i] = &bucket[K, V]{
			m: make(map[K]V),
		}
	}
	return &ConcurrentDict[K, V]{
		buckets:   buckets,
		bucketNum: bucketNum,
		length:    0,
	}
}

// 计算合适的bucket个数
func computeBucketNum(param int32) (size int32) {
	if param <= 16 {
		return 16 // bucket个数最少为16
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
	return n + 1 // 返回的数一定是2的幂
}

// 根据key计算并获取该key应该存放的bucket
func (dict *ConcurrentDict[K, V]) getBucket(key K) *bucket[K, V] {
	hashCode := fnv.Fnv32(key) // fnv算法计算哈希值
	index := (uint32(dict.bucketNum) - 1) & hashCode
	return dict.buckets[index]
}

func (dict *ConcurrentDict[K, V]) Len() int {
	checkNilDict(dict)
	return int(atomic.LoadInt32(&dict.length)) // atomically
}

func (dict *ConcurrentDict[K, V]) ContainKey(key K) bool {
	checkNilDict(dict)
	bucket := dict.getBucket(key)
	// 获取读锁
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	_, existed := bucket.m[key]
	return existed
}

func (dict *ConcurrentDict[K, V]) Get(key K) (val V, existed bool) {
	checkNilDict(dict)
	bucket := dict.getBucket(key)
	// 获取读锁
	bucket.lock.RLock()
	defer bucket.lock.RUnlock()

	val, existed = bucket.m[key]
	return
}

func (dict *ConcurrentDict[K, V]) Put(key K, val V) (result int) {
	checkNilDict(dict)
	bucket := dict.getBucket(key)
	// 获取写锁
	bucket.lock.Lock()
	defer bucket.lock.Unlock()

	if _, ok := bucket.m[key]; ok {
		bucket.m[key] = val
		return 0
	}
	atomic.AddInt32(&dict.length, 1)
	bucket.m[key] = val
	return 1
}

func (dict *ConcurrentDict[K, V]) PutIfAbsent(key K, val V) (result int) {
	checkNilDict(dict)
	bucket := dict.getBucket(key)
	// 获取写锁
	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	_, existed := bucket.m[key]
	if existed {
		return 0 // 已存在
	} else {
		bucket.m[key] = val              // 未存在
		atomic.AddInt32(&dict.length, 1) // atomically
		return 1
	}
}

func (dict *ConcurrentDict[K, V]) PutIfExists(key K, val V) (result int) {
	checkNilDict(dict)
	bucket := dict.getBucket(key)
	// 获取写锁
	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	_, existed := bucket.m[key]
	if existed {
		bucket.m[key] = val // 已存在
		return 1
	} else {
		return 0 //未存在
	}
}

func (dict *ConcurrentDict[K, V]) Remove(key K) (result int) {
	checkNilDict(dict)
	bucket := dict.getBucket(key)
	// 获取写锁
	bucket.lock.Lock()
	defer bucket.lock.Unlock()
	_, existed := bucket.m[key]
	if existed {
		delete(bucket.m, key)             // 已存在
		atomic.AddInt32(&dict.length, -1) // atomically
		return 1
	} else {
		return 0 // 未存在
	}
}

func (dict *ConcurrentDict[K, V]) ForEach(consumer Consumer[K, V]) {
	checkNilDict(dict)
	for _, bucket := range dict.buckets {
		// 使用匿名函数是为了 bucket.lock.RUnlock() 在本循环内即可执行
		func() {
			// 一个循环内只对一个bucket加锁
			bucket.lock.RLock()
			defer bucket.lock.RUnlock()
			for key, val := range bucket.m {
				continues := consumer(key, val)
				if !continues {
					return
				}
			}
		}()
	}
}

func (dict *ConcurrentDict[K, V]) RealForEach(consumer Consumer[K, V]) {
	checkNilDict(dict)
	// 在ForEach之前需要获取所有bucket的锁
	for _, bucket := range dict.buckets {
		bucket.lock.RLock()
	}
	for _, bucket := range dict.buckets {
		for key, val := range bucket.m {
			continues := consumer(key, val)
			if !continues {
				return
			}
		}
		bucket.lock.RUnlock() // 访问过的bucket可以立即解锁
	}
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

func (dict *ConcurrentDict[K, V]) Values() []V {
	checkNilDict(dict)
	vals := make([]V, dict.Len())
	i := 0
	dict.ForEach(func(key K, val V) bool {
		if i < len(vals) {
			vals[i] = val
			i++
		} else {
			vals = append(vals, val)
		}
		return true
	})
	return vals
}

func (dict *ConcurrentDict[K, V]) RealKeys() []K {
	checkNilDict(dict)
	keys := make([]K, dict.Len())
	i := 0
	dict.RealForEach(func(key K, val V) bool {
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

func (dict *ConcurrentDict[K, V]) RealValues() []V {
	checkNilDict(dict)
	vals := make([]V, dict.Len())
	i := 0
	dict.RealForEach(func(key K, val V) bool {
		if i < len(vals) {
			vals[i] = val
			i++
		} else {
			vals = append(vals, val)
		}
		return true
	})
	return vals
}

func (dict *ConcurrentDict[K, V]) RandomKeys(num int) []K {
	checkNilDict(dict)
	size := dict.Len()
	if num >= size {
		return dict.Keys()
	}
	keys := make([]K, num)
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	i := 0
	for i < num {
		index := seed.Intn(int(dict.bucketNum)) // 获取一个随机下标
		bucket := dict.buckets[index]           // 随机bucket
		res := bucket.randomKey()               // 从bucket中获取一个随机key，结果可能为nil
		if res != nil {
			keys[i] = res[0]
			i++
		}
	}
	return keys
}

func (dict *ConcurrentDict[K, V]) RandomDistinctKeys(num int) []K {
	checkNilDict(dict)
	size := dict.Len()
	if num >= size {
		return dict.Keys()
	}
	keys := make(map[K]bool) // 使用map去重
	seed := rand.New(rand.NewSource(time.Now().UnixNano()))
	for len(keys) < num {
		index := seed.Intn(int(dict.bucketNum)) // 获取一个随机下标
		bucket := dict.buckets[index]           // 随机bucket
		res := bucket.randomKey()               // 从bucket中获取一个随机key，结果可能为nil
		if res != nil {
			keys[res[0]] = true
		}
	}
	arr := make([]K, num)
	i := 0
	for k := range keys {
		arr[i] = k
		i++
	}
	return arr
}

func (dict *ConcurrentDict[K, V]) Clear() {
	checkNilDict(dict)
	*dict = *NewConcurrentDict[K, V](dict.bucketNum)
}

func checkNilDict(dict any) {
	if dict == nil {
		panic("dict is nil")
	}
}

package dict

type Dict[K comparable, V any] interface {
	Len() int
	ContainKey(key K) bool
	Get(key K) (val V, existed bool)
	Put(key K, val V) (result int)         // 添加键值对，并返回实际添加的个数
	PutIfAbsent(key K, val V) (result int) // put当且仅当不存在，并返回实际添加的个数
	PutIfExists(key K, val V) (result int) // put当且仅当已经存在，并返回实际添加的个数
	Remove(key K) (result int)             // 移除键值对，并返回实际移除的个数

	ForEach(consumer Consumer[K, V]) // 迭代器
	Keys() []K
	Values() []V

	RealForEach(consumer Consumer[K, V]) // "Real"迭代器，针对concurrent_dict
	RealKeys() []K
	RealValues() []V

	RandomKeys(num int) []K         // 随机获取指定个数的key，且key可以重复
	RandomDistinctKeys(num int) []K // 随机获取指定个数的key，且所有key都唯一
	Clear()
}

type Consumer[K comparable, V any] func(key K, val V) bool

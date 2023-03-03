package dict

// SimpleDict 对map的简单包装，实现了 Dict 接口
type SimpleDict[K comparable, V any] struct {
	m map[K]V
}

func MakeSimpleDict[K comparable, V any]() *SimpleDict[K, V] {
	return &SimpleDict[K, V]{make(map[K]V)}
}

func (dict *SimpleDict[K, V]) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	return len(dict.m)
}

func (dict *SimpleDict[K, V]) ContainKey(key K) bool {
	if dict == nil {
		panic("dict is nil")
	}
	_, existed := dict.m[key]
	return existed
}

func (dict *SimpleDict[K, V]) Get(key K) (val V, existed bool) {
	if dict == nil {
		panic("dict is nil")
	}
	val, existed = dict.m[key]
	return
}

func (dict *SimpleDict[K, V]) Put(key K, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	_, existed := dict.m[key]
	dict.m[key] = val
	if existed {
		return 0
	}
	return 1
}

func (dict *SimpleDict[K, V]) PutIfAbsent(key K, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	if _, existed := dict.m[key]; existed {
		return 0
	}
	dict.m[key] = val
	return 1
}

func (dict *SimpleDict[K, V]) PutIfExists(key K, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	if _, existed := dict.m[key]; existed {
		dict.m[key] = val
		return 1
	}
	return 0
}

func (dict *SimpleDict[K, V]) Remove(key K) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	_, existed := dict.m[key]
	if existed {
		delete(dict.m, key)
		return 1
	}
	return 0
}

func (dict *SimpleDict[K, V]) Keys() []K {
	if dict == nil {
		panic("dict is nil")
	}
	keys := make([]K, len(dict.m))
	i := 0
	for key := range dict.m {
		keys[i] = key
		i++
	}
	return keys
}

func (dict *SimpleDict[K, V]) Values() []V {
	if dict == nil {
		panic("dict is nil")
	}
	vals := make([]V, len(dict.m))
	i := 0
	for _, val := range dict.m {
		vals[i] = val
		i++
	}
	return vals
}

func (dict *SimpleDict[K, V]) RealKeys() []K {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.Keys()
}

func (dict *SimpleDict[K, V]) RealValues() []V {
	if dict == nil {
		panic("dict is nil")
	}
	return dict.Values()
}

func (dict *SimpleDict[K, V]) RandomKeys(num int) []K {
	if dict == nil {
		panic("dict is nil")
	}
	if num < 0 {
		panic("error: number is less than zero")
	}
	res := make([]K, num)
	for i := 0; i < num; i++ {
		for key := range dict.m {
			res[i] = key
			break // 每次只获取第一个key
		}
	}
	return res
}

func (dict *SimpleDict[K, V]) RandomDistinctKeys(num int) []K {
	if dict == nil {
		panic("dict is nil")
	}
	if num < 0 {
		panic("error: number is less than zero")
	}
	if num > len(dict.m) {
		num = len(dict.m)
	}
	res := make([]K, num)
	i := 0
	for key := range dict.m {
		res[i] = key
		i++
		if i == num {
			break
		}
	}
	return res
}

func (dict *SimpleDict[K, V]) ForEach(consumer Consumer[K, V]) {
	if dict == nil {
		panic("dict is nil")
	}
	for key, val := range dict.m {
		if !consumer(key, val) {
			break
		}
	}
}

func (dict *SimpleDict[K, V]) RealForEach(consumer Consumer[K, V]) {
	if dict == nil {
		panic("dict is nil")
	}
	dict.ForEach(consumer)
}

func (dict *SimpleDict[K, V]) Clear() {
	if dict == nil {
		panic("dict is nil")
	}
	//for key := range dict.m {
	//	delete(dict.m, key)
	//}
	*dict = *MakeSimpleDict[K, V]()
}

package dict

// SimpleDict 对map的简单包装，实现了 Dict 接口
type SimpleDict[K comparable, V any] struct {
	m map[K]V
}

func MakeSimpleDict[K comparable, V any]() *SimpleDict[K, V] {
	return &SimpleDict[K, V]{make(map[K]V)}
}

func (dict *SimpleDict[K, V]) checkNil() {
	if dict.m == nil {
		panic("m is nil")
	}
}

func (dict *SimpleDict[K, V]) Len() int {
	if dict == nil {
		panic("dict is nil")
	}
	dict.checkNil()
	return len(dict.m)
}

func (dict *SimpleDict[K, V]) Get(key K) (val V, existed bool) {
	if dict == nil {
		panic("dict is nil")
	}
	dict.checkNil()
	val, existed = dict.m[key]
	return
}

func (dict *SimpleDict[K, V]) Put(key K, val V) (result int) {
	if dict == nil {
		panic("dict is nil")
	}
	dict.checkNil()
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
	dict.checkNil()
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
	dict.checkNil()
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
	dict.checkNil()
	delete(dict.m, key)
	if _, existed := dict.m[key]; existed {
		delete(dict.m, key)
		return 1
	}
	return 0
}

func (dict *SimpleDict[K, V]) Keys() []K {
	if dict == nil {
		panic("dict is nil")
	}
	dict.checkNil()
	res := make([]K, len(dict.m))
	i := 0
	for key := range dict.m {
		res[i] = key
		i++
	}
	return res
}

func (dict *SimpleDict[K, V]) RandomKeys(num int) []K {
	if dict == nil {
		panic("dict is nil")
	}
	dict.checkNil()
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
	dict.checkNil()
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
	dict.checkNil()
	for key, val := range dict.m {
		if !consumer(key, val) {
			break
		}
	}
}

func (dict *SimpleDict[K, V]) Clear() {
	if dict == nil {
		panic("dict is nil")
	}
	dict.checkNil()
	//for key := range dict.m {
	//	delete(dict.m, key)
	//}
	*dict = *MakeSimpleDict[K, V]()
}

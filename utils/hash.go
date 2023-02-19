package utils

import "fmt"

const prime32 = uint32(16777619)

func Fnv32[K any](key K) uint32 {
	str := fmt.Sprintf("%v", key) // key转化为字符串，以便hash
	hash := uint32(2166136261)
	for i := 0; i < len(str); i++ {
		hash *= prime32
		hash ^= uint32(str[i])
	}
	return hash
}

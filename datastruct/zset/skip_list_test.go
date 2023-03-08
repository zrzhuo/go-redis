package zset

import (
	"testing"
)

func TestRandomLevel(t *testing.T) {
	counter := make(map[int16]int)
	for i := 0; i < 10000; i++ {
		level := randomLevel()
		counter[level]++
	}
	for i := 0; i <= maxLevel; i++ {
		t.Logf("levels %d, count %d", i, counter[int16(i)])
	}
}

func TestMakeSkipList(t *testing.T) {
	comp := func(a string, b string) int {
		if a < b {
			return -1
		} else if a > b {
			return 1
		} else {
			return 0
		}
	}
	sl := MakeSkiplist[string](comp)
	sl.Insert("aaa", 1)
	sl.Insert("bbb", 2)
	sl.Insert("ccc", 2)
	sl.Insert("bba", 2)
	sl.print()
	println("---------------------")
	sl.Insert("bbb", 2)
	sl.print()

}

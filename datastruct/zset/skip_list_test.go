package zset

import (
	"strconv"
	"testing"
)

func TestRandomLevel(t *testing.T) {
	counter := make(map[int16]int)
	for i := 0; i < 100000; i++ {
		level := randomLevel()
		counter[level]++
	}
	for key, val := range counter {
		t.Logf("levels %d, count %d", key, val)
	}
}

var comp = func(a string, b string) int {
	if a < b {
		return -1
	} else if a > b {
		return 1
	} else {
		return 0
	}
}

func TestSkipList_Insert(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 1000; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	sl.print()
}

func TestSkipList_Remove(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	for i := 3; i < 8; i++ {
		sl.Remove(strconv.FormatInt(int64(i), 10), float64(i))
	}
	sl.print()
}

func TestSkipList_RemoveRangeByScore(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	res := sl.RemoveRangeByScore(3, 8, 3)
	for _, node := range res {
		t.Logf("%v : %f", node.Obj, node.Score)
	}
	sl.print()
}

func TestSkipList_RemoveRangeByRank(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	res := sl.RemoveRangeByRank(3, 8)
	for _, node := range res {
		t.Logf("%v : %f", node.Obj, node.Score)
	}
	sl.print()
}

func TestSkipList_GetRank(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	for i := 0; i < 10; i++ {
		println(sl.GetRank(strconv.FormatInt(int64(i), 10), float64(i)))
	}
}

func TestSkipList_GetNode(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	for i := 0; i < 10; i++ {
		node := sl.GetNode(strconv.FormatInt(int64(i), 10), float64(i))
		println(node.Obj, node.Score)
	}
}
func TestSkipList_GetNodeByRank(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	for i := 0; i < 10; i++ {
		node := sl.GetNodeByRank(i)
		println(node.Obj, node.Score)
	}
}

func TestSkipList_FirstInRange(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	println(sl.FirstInRange(3, 8).Obj)
}

func TestSkipList_LastInRange(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	println(sl.LastInRange(3, 8).Obj)
}

func TestSkipList_CountRange(t *testing.T) {
	sl := NewSkiplist[string](comp)
	for i := 0; i < 10; i++ {
		sl.Insert(strconv.FormatInt(int64(i), 10), float64(i))
	}
	println(sl.CountRange(3, 8))
}

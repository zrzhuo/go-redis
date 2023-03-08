package zset

import (
	"fmt"
	"math"
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 32
)

type skipLevel[T comparable] struct {
	next *SkipNode[T] // 前进指针，指向该层索引的下一个结点
	span int          // 跨度，距离下一个结点距离
}

type SkipNode[T comparable] struct {
	Obj    T
	Score  float64
	prev   *SkipNode[T]    // 后退指针，指向前一个结点
	levels []*skipLevel[T] // 索引，level[0]跨度为1
}

type SkipList[T comparable] struct {
	header *SkipNode[T] // 虚拟结点，其levels中的next指向第一个实际结点，score为最小值
	tail   *SkipNode[T] // 直接指向最后一个实际结点
	length int          // 结点数量
	level  int16        // 最大索引层级数，不包括虚拟头结点
	comp   Compare[T]   // 比较T的函数，用于Score相等时比较Obj
}

func newSkipNode[T comparable](obj T, score float64, level int16) *SkipNode[T] {
	node := &SkipNode[T]{
		Obj:    obj,
		Score:  score,
		levels: make([]*skipLevel[T], level),
	}
	for i := range node.levels {
		node.levels[i] = &skipLevel[T]{}
	}
	return node
}

func MakeSkiplist[T comparable](comp Compare[T]) *SkipList[T] {
	var null T
	return &SkipList[T]{
		header: newSkipNode[T](null, math.SmallestNonzeroFloat64, maxLevel), // 头结点的score设为最小值
		tail:   nil,                                                         // 尾指针指向nil
		level:  1,                                                           // 初始为1
		comp:   comp,                                                        // 比较函数
	}
}

// 随机生成一个[1,maxLevel]之间的整数，满足幂次定律，即越大的数生成概率越小（长尾）
func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return (maxLevel-int16(bits.Len64(k)))%maxLevel + 1
}

func (sl *SkipList[T]) Len() int {
	if sl == nil {
		panic("this SkipList is nil")
	}
	return sl.length
}

func (sl *SkipList[T]) Insert(obj T, score float64) {
	if sl == nil {
		panic("this SkipList is nil")
	}
	condition := func(node *SkipNode[T]) bool {
		return node.Score < score || (node.Score == score && sl.comp(node.Obj, obj) == -1)
	}
	// 寻找插入位置
	prevs := make([]*SkipNode[T], maxLevel) // 记录新结点在每一层中的前置节点
	ranks := make([]int, maxLevel)          // 记录新结点在每一层中的rank
	rank := 0
	node := sl.header
	// 遍历跳表
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && condition(node.levels[i].next) {
			rank += node.levels[i].span // 累计rank
			node = node.levels[i].next  // 向后移动
		}
		ranks[i] = rank // 记录rank
		prevs[i] = node // 记录前置节点
	}
	// 建立新结点
	level := randomLevel() // 随机层高
	newNode := newSkipNode(obj, score, level)
	if level > sl.level {
		// 此时需要新增索引层
		for i := sl.level; i < level; i++ {
			sl.header.levels[i].span = sl.length // 新增的索引层中，header的span都为结点总数
			prevs[i] = sl.header                 // 新增的索引层中，当前结点的前置结点都为header
			ranks[i] = 0                         // 新增的索引层中，当前结点的的rank都为0
		}
		sl.level = level
	}
	// 插入新结点
	sl.insertNode(newNode, prevs, ranks)
}

func (sl *SkipList[T]) Remove(obj T, score float64) bool {
	if sl == nil {
		panic("this SkipList is nil")
	}
	condition := func(node *SkipNode[T]) bool {
		return node.Score < score || (node.Score == score && sl.comp(node.Obj, obj) == -1)
	}
	// 寻找删除位置
	prevs := make([]*SkipNode[T], maxLevel) // 记录目标结点在每一层索引中的前置结点
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && condition(node.levels[i].next) {
			node = node.levels[i].next
		}
		prevs[i] = node
	}
	// 下一个结点即为目标结点
	tarNode := node.levels[0].next
	if tarNode != nil && tarNode.Score == score && sl.comp(tarNode.Obj, obj) == 0 {
		sl.removeNode(tarNode, prevs) // 找到目标结点，删除之
		return true
	}
	return false
}

// RemoveRangeByScore 移除score在[min, max]中的结点，最多移除num个，并返回这些结点
func (sl *SkipList[T]) RemoveRangeByScore(min float64, max float64, num int) []*SkipNode[T] {
	if sl == nil {
		panic("this SkipList is nil")
	}
	prevs := make([]*SkipNode[T], maxLevel) // 记录目标结点在每一层索引中的前置结点
	// 寻找最后一个score<min的结点
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && node.levels[i].next.Score < min {
			node = node.levels[i].next
		}
		prevs[i] = node
	}
	// 从下一个结点开始删除
	count := 0
	result := make([]*SkipNode[T], 0)
	node = node.levels[0].next
	for count < num && node != nil && node.Score <= max {
		next := node.levels[0].next
		sl.removeNode(node, prevs)
		result = append(result, node)
		count++
		node = next
	}
	return result
}

// RemoveRangeByRank 移除rank在[start, stop)之内的所有结点
func (sl *SkipList[T]) RemoveRangeByRank(start int, stop int) []*SkipNode[T] {
	if sl == nil {
		panic("this SkipList is nil")
	}
	prevs := make([]*SkipNode[T], maxLevel) // 记录目标结点在每一层索引中的前置结点
	// 寻找最后一个rank<start的结点
	rank := -1
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && rank+node.levels[i].span < start {
			rank += node.levels[i].span
			node = node.levels[i].next
		}
		prevs[i] = node
	}
	// 从下一个结点开始删除
	result := make([]*SkipNode[T], 0)
	rank += 1
	node = node.levels[0].next
	for node != nil && rank < stop {
		next := node.levels[0].next
		sl.removeNode(node, prevs)
		result = append(result, node)
		rank += 1
		node = next
	}
	return result
}

// GetRank 返回指定成员的rank，rank从0开始
func (sl *SkipList[T]) GetRank(obj T, score float64) int {
	if sl == nil {
		panic("this SkipList is nil")
	}
	condition := func(node *SkipNode[T]) bool {
		return node.Score < score || (node.Score == score && sl.comp(node.Obj, obj) == -1)
	}
	rank := -1 // header的rank为-1
	node := sl.header
	// 寻找目标结点的可能前置结点
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && condition(node.levels[i].next) {
			rank += node.levels[i].span
			node = node.levels[i].next
		}
	}
	// 判断下一个结点是否符合要求
	tarNode := node.levels[0].next
	rank += 1
	if tarNode != nil && tarNode.Score == score && sl.comp(tarNode.Obj, obj) == 0 {
		return rank
	}
	return -1 // -1表示不包含该成员
}

// GetNode 根据obj和score获取节点
func (sl *SkipList[T]) GetNode(obj T, score float64) *SkipNode[T] {
	if sl == nil {
		panic("this SkipList is nil")
	}
	condition := func(node *SkipNode[T]) bool {
		return node.Score < score || (node.Score == score && sl.comp(node.Obj, obj) == -1)
	}
	node := sl.header
	// 寻找目标结点的可能前置结点
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && condition(node.levels[i].next) {
			node = node.levels[i].next
		}
	}
	// 判断下一个结点是否符合要求
	node = node.levels[0].next
	if node != nil && node.Score == score && sl.comp(node.Obj, obj) == 0 {
		return node
	}
	return nil
}

// GetNodeByRank 根据rank获取节点
func (sl *SkipList[T]) GetNodeByRank(tarRank int) *SkipNode[T] {
	if sl == nil {
		panic("this SkipList is nil")
	}
	if tarRank < 0 || tarRank >= sl.length {
		return nil
	}
	rank := -1 // header的rank为-1
	node := sl.header
	// 寻找最后一个rank<targetRank的结点
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && rank+node.levels[i].span < tarRank {
			rank = rank + node.levels[i].span
			node = node.levels[i].next
		}
	}
	return node.levels[0].next
}

// IsInRange 判断是否存在score位于[min, max]的结点
func (sl *SkipList[T]) IsInRange(min float64, max float64) bool {
	if sl == nil {
		panic("this SkipList is nil")
	}
	if min > max {
		return false
	}
	lastNode := sl.tail
	if lastNode == nil || min > lastNode.Score {
		return false
	}
	firstNode := sl.header.levels[0].next
	if firstNode == nil || max < firstNode.Score {
		return false
	}
	return true
}

// FirstInRange 返回score位于[min, max]中的第一个结点
func (sl *SkipList[T]) FirstInRange(min float64, max float64) *SkipNode[T] {
	if sl == nil {
		panic("this SkipList is nil")
	}
	if !sl.IsInRange(min, max) {
		return nil
	}
	node := sl.header
	// 寻找最后一个score<min的结点
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && node.levels[i].next.Score < min {
			node = node.levels[i].next
		}
	}
	return node.levels[0].next
}

// LastInRange 返回score位于[min, max]中的最后一个结点
func (sl *SkipList[T]) LastInRange(min float64, max float64) *SkipNode[T] {
	if sl == nil {
		panic("this SkipList is nil")
	}
	if !sl.IsInRange(min, max) {
		return nil
	}
	node := sl.header
	// 寻找最后一个score<=max的结点
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && node.levels[i].next.Score <= max {
			node = node.levels[i].next
		}
	}
	return node
}

// CountRange 返回score位于[min, max]中的结点个数
func (sl *SkipList[T]) CountRange(min float64, max float64) int {
	if sl == nil {
		panic("this SkipList is nil")
	}
	prevs := make([]*SkipNode[T], maxLevel) // 记录目标结点在每一层索引中的前置结点
	// 寻找最后一个score<min的结点
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for node.levels[i].next != nil && node.levels[i].next.Score < min {
			node = node.levels[i].next
		}
		prevs[i] = node
	}
	// 从下一个结点开始计数
	count := 0
	node = node.levels[0].next
	for node != nil && node.Score <= max {
		count++
		node = node.levels[0].next
	}
	return count
}

func (sl *SkipList[T]) insertNode(newNode *SkipNode[T], prevs []*SkipNode[T], ranks []int) {
	level := int16(len(newNode.levels))
	// 连接next、更新span
	for i := int16(0); i < level; i++ {
		newNode.levels[i].next = prevs[i].levels[i].next
		prevs[i].levels[i].next = newNode
		newNode.levels[i].span = prevs[i].levels[i].span - (ranks[0] - ranks[i])
		prevs[i].levels[i].span = (ranks[0] - ranks[i]) + 1
	}
	// 对于未涉及的高层前置结点，span加1
	for i := level; i < sl.level; i++ {
		prevs[i].levels[i].span++
	}
	// 连接prev
	if prevs[0] == sl.header {
		newNode.prev = nil
	} else {
		newNode.prev = prevs[0]
	}
	if newNode.levels[0].next == nil {
		sl.tail = newNode
	} else {
		newNode.levels[0].next.prev = newNode
	}
	// 长度加1
	sl.length++
}

func (sl *SkipList[T]) removeNode(tarNode *SkipNode[T], prevs []*SkipNode[T]) {
	// 断开next
	for i := int16(0); i < sl.level; i++ {
		if prevs[i].levels[i].next == tarNode {
			// tarNode存在于该层索引
			prevs[i].levels[i].span += tarNode.levels[i].span - 1 // 更新前置结点的span
			prevs[i].levels[i].next = tarNode.levels[i].next      // 更新前置结点的next
		} else {
			// tarNode不存在于该层索引
			prevs[i].levels[i].span--
		}
	}
	// 断开prev
	if tarNode.levels[0].next != nil {
		// tarNode不是最后一个节点
		tarNode.levels[0].next.prev = tarNode.prev
	} else {
		// tarNode是最后一个节点
		sl.tail = tarNode.prev
	}
	// 最高层索引没有结点时，索引层数减1
	for sl.level > 1 && sl.header.levels[sl.level-1].next == nil {
		sl.level--
	}
	// 长度减1
	sl.length--
}

func (sl *SkipList[T]) print() {
	for curr := sl.header.levels[0].next; curr != nil; curr = curr.levels[0].next {
		fmt.Println(curr.Obj, curr.Score)
	}
}

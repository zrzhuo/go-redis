package zset

import (
	"fmt"
	"math/bits"
	"math/rand"
)

const (
	maxLevel = 16
)

type skipLevel[T comparable] struct {
	next *skipNode[T] // 指向该层索引的下一个结点
	span int          // 该层索引的跨度
}

type skipNode[T comparable] struct {
	Member T
	Score  float64
	prev   *skipNode[T]    // 指向前一个结点
	levels []*skipLevel[T] // 索引，level[0]跨度为1
}

type SkipList[T comparable] struct {
	header *skipNode[T] // 虚拟结点，其levels中的next指向实际结点，score为最小值0
	tail   *skipNode[T] // 直接指向最后一个结点
	size   int          // 结点数量
	level  int16        // 最大索引层级
	comp   Compare[T]   // 比较member的函数
}

func newSkipNode[T comparable](member T, score float64, level int16) *skipNode[T] {
	node := &skipNode[T]{
		Member: member,
		Score:  score,
		levels: make([]*skipLevel[T], level),
	}
	for i := range node.levels {
		node.levels[i] = &skipLevel[T]{
			next: nil,
			span: 0,
		}
	}
	return node
}

func MakeSkiplist[T comparable](comp Compare[T]) *SkipList[T] {
	var null T
	return &SkipList[T]{
		header: newSkipNode[T](null, 0, maxLevel), // 头结点的score为0
		tail:   nil,                               // 尾指针指向nil
		level:  1,                                 // 初始为1
		comp:   comp,                              // 比较函数
	}
}

func randomLevel() int16 {
	total := uint64(1)<<uint64(maxLevel) - 1
	k := rand.Uint64() % total
	return maxLevel - int16(bits.Len64(k)) + 1
}

func (sl *SkipList[T]) print() {
	for curr := sl.header.levels[0].next; curr != nil; curr = curr.levels[0].next {
		fmt.Println(curr.Member, curr.Score)
	}
}

func (sl *SkipList[T]) Len() int {
	if sl == nil {
		panic("this SkipList is nil.")
	}
	return sl.size
}

func (sl *SkipList[T]) Insert(member T, score float64) {
	prevs := make([]*skipNode[T], maxLevel) // 记录新结点在每一层中的插入位置，即新结点在该层索引中的前置结点
	ranks := make([]int, maxLevel)          // 记录新结点在每一层中的rank（第一个实际结点的rank为0）

	// 1、寻找插入位置
	prevNode := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		if i == sl.level-1 {
			ranks[i] = 0 // 最高层的rank初始为0
		} else {
			ranks[i] = ranks[i+1] // 初始化为上一层的rank
		}
		// prevNode.levels[i] != nil，标志着preNode结点是否被该层索引
		if prevNode.levels[i] != nil {
			for ptr := prevNode.levels[i].next; ptr != nil; ptr = ptr.levels[i].next {
				if ptr.Score < score || (ptr.Score == score && sl.comp(ptr.Member, member) == -1) {
					// 当前结点的score小于新结点score || score相等，但当前结点的member值小于新结点member
					ranks[i] += prevNode.levels[i].span // 更新ranks
					prevNode = prevNode.levels[i].next  // 向后移动
				} else {
					break // 此时，prevNode结点即为新结点在该层的前置结点
				}
			}
		}
		prevs[i] = prevNode // 记录前置节点
	}

	// 2、建立新结点
	level := randomLevel()
	newNode := newSkipNode(member, score, level)
	if level > sl.level {
		// 此时需要新增索引层
		for i := sl.level; i < level; i++ {
			sl.header.levels[i].span = sl.size // 新增的索引层中，header的span都为结点总数
			prevs[i] = sl.header               // 新增的索引层中，当前结点的前置结点都为header
			ranks[i] = 0                       // 新增的索引层中，当前结点的的rank都为0
		}
		sl.level = level
	}

	// 3、插入新结点
	sl.insertNode(newNode, prevs, ranks)
}

func (sl *SkipList[T]) insertNode(newNode *skipNode[T], prevs []*skipNode[T], ranks []int) {
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
	sl.size++
}

func (sl *SkipList[T]) Remove(member T, score float64) bool {
	// 寻找删除位置
	prevs := make([]*skipNode[T], maxLevel) // 记录目标结点在每一层索引中的前置结点
	prevNode := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for ptr := prevNode.levels[i].next; ptr != nil; ptr = ptr.levels[i].next {
			if ptr.Score < score || (ptr.Score == score && sl.comp(ptr.Member, member) == -1) {
				// 当前结点的score小于新结点score || score相等，但当前结点的member值小于目标结点member
				prevNode = prevNode.levels[i].next
			} else {
				break
			}
		}
		prevs[i] = prevNode
	}
	tarNode := prevs[0].levels[0].next
	if tarNode != nil && tarNode.Score == score && sl.comp(tarNode.Member, member) == 0 {
		// 找到目标结点，删除之
		sl.removeNode(tarNode, prevs)
		return true
	}
	return false
}

func (sl *SkipList[T]) removeNode(tarNode *skipNode[T], prevs []*skipNode[T]) {
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
	sl.size--
}

// GetRank 返回指定成员的rank，rank从0开始
func (sl *SkipList[T]) GetRank(member T, score float64) int {
	rank := -1
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for ptr := node.levels[i].next; ptr != nil; ptr = ptr.levels[i].next {
			if ptr.Score < score ||
				(ptr.Score == score && sl.comp(ptr.Member, member) != 1) {
				// 当前结点的score小于新结点score || score相等，但当前结点的member值小于等于目标结点member
				rank += node.levels[i].span
				node = node.levels[i].next
			} else {
				break
			}
		}
		if node.Member == member {
			return rank
		}
	}
	return -1 // -1表示不包含该成员
}

func (sl *SkipList[T]) getNodeByRank(rank int) *skipNode[T] {
	r := -1
	node := sl.header
	for i := sl.level - 1; i >= 0; i-- {
		for ptr := node.levels[i].next; ptr != nil; ptr = ptr.levels[i].next {
			if r+node.levels[i].span <= rank {
				r += node.levels[i].span
				node = node.levels[i].next
			} else {
				break
			}
		}
		if r == rank {
			return node
		}
	}
	return nil // 不存在
}

//func (sl *SkipList) hasInRange(min *ScoreBorder, max *ScoreBorder) bool {
//	// min & max = empty
//	if min.Value > max.Value || (min.Value == max.Value && (min.Exclude || max.Exclude)) {
//		return false
//	}
//	// min > tail
//	n := sl.tail
//	if n == nil || !min.less(n.Score) {
//		return false
//	}
//	// max < header
//	n = sl.header.levels[0].next
//	if n == nil || !max.greater(n.Score) {
//		return false
//	}
//	return true
//}
//
//func (sl *SkipList) getFirstInScoreRange(min *ScoreBorder, max *ScoreBorder) *skipNode {
//	if !sl.hasInRange(min, max) {
//		return nil
//	}
//	n := sl.header
//	// scan from top levels
//	for level := sl.level - 1; level >= 0; level-- {
//		// if next is not in range than move next
//		for n.levels[level].next != nil && !min.less(n.levels[level].next.Score) {
//			n = n.levels[level].next
//		}
//	}
//	/* This is an inner range, so the next skipNode cannot be NULL. */
//	n = n.levels[0].next
//	if !max.greater(n.Score) {
//		return nil
//	}
//	return n
//}
//
//func (sl *SkipList) getLastInScoreRange(min *ScoreBorder, max *ScoreBorder) *skipNode {
//	if !sl.hasInRange(min, max) {
//		return nil
//	}
//	n := sl.header
//	// scan from top levels
//	for level := sl.level - 1; level >= 0; level-- {
//		for n.levels[level].next != nil && max.greater(n.levels[level].next.Score) {
//			n = n.levels[level].next
//		}
//	}
//	if !min.less(n.Score) {
//		return nil
//	}
//	return n
//}
//
///*
// * return removed elements
// */
//func (sl *SkipList) RemoveRangeByScore(min *ScoreBorder, max *ScoreBorder, limit int) (removed []*Element) {
//	update := make([]*skipNode, maxLevel)
//	removed = make([]*Element, 0)
//	// find prev nodes (of target range) or last skipNode of each levels
//	node := sl.header
//	for i := sl.level - 1; i >= 0; i-- {
//		for node.levels[i].next != nil {
//			if min.less(node.levels[i].next.Score) { // already in range
//				break
//			}
//			node = node.levels[i].next
//		}
//		update[i] = node
//	}
//
//	// skipNode is the first one within range
//	node = node.levels[0].next
//
//	// remove nodes in range
//	for node != nil {
//		if !max.greater(node.Score) { // already out of range
//			break
//		}
//		next := node.levels[0].next
//		removedElement := node.Element
//		removed = append(removed, &removedElement)
//		sl.removeNode(node, update)
//		if limit > 0 && len(removed) == limit {
//			break
//		}
//		node = next
//	}
//	return removed
//}
//
//// 1-based rank, including start, exclude stop
//func (sl *SkipList) RemoveRangeByRank(start int64, stop int64) (removed []*Element) {
//	var i int64 = 0 // rank of iterator
//	update := make([]*skipNode, maxLevel)
//	removed = make([]*Element, 0)
//
//	// scan from top levels
//	node := sl.header
//	for level := sl.level - 1; level >= 0; level-- {
//		for node.levels[level].next != nil && (i+node.levels[level].span) < start {
//			i += node.levels[level].span
//			node = node.levels[level].next
//		}
//		update[level] = node
//	}
//
//	i++
//	node = node.levels[0].next // first skipNode in range
//
//	// remove nodes in range
//	for node != nil && i < stop {
//		next := node.levels[0].next
//		removedElement := node.Element
//		removed = append(removed, &removedElement)
//		sl.removeNode(node, update)
//		node = next
//		i++
//	}
//	return removed
//}

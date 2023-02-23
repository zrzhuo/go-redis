package list

import (
	"fmt"
	"strings"
)

// Node DLinkedList的结点
type Node[T any] struct {
	val  T        // 结点值
	prev *Node[T] // 指向前一个节点
	next *Node[T] // 指向后一个节点
}

// DLinkedList 链表定义
type DLinkedList[T any] struct {
	head *Node[T] // 指向第一个结点
	tail *Node[T] // 指向最后一个结点
	size int      // 链表当前含有的结点数
}

func MakeDLinkedList[T any]() *DLinkedList[T] {
	return &DLinkedList[T]{
		head: nil,
		tail: nil,
		size: 0,
	}
}

func (list *DLinkedList[T]) Len() int {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	return list.size
}

func (list *DLinkedList[T]) Add(val T) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	var curr = new(Node[T])
	curr.val = val
	if list.head == nil {
		// 链表为空时
		list.head = curr
		list.tail = curr
	} else {
		// 链表不为空时
		list.tail.next = curr
		curr.prev = list.tail
		list.tail = curr
	}
	list.size++
}

func (list *DLinkedList[T]) Get(idx int) T {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	list.boundCheck(idx)
	return list.find(idx).val
}

func (list *DLinkedList[T]) GetFirst() T {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	return list.find(0).val
}

func (list *DLinkedList[T]) GetLast() T {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	return list.find(list.size - 1).val
}

func (list *DLinkedList[T]) Set(idx int, val T) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	list.boundCheck(idx)
	list.find(idx).val = val
}

func (list *DLinkedList[T]) SetFirst(val T) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	list.find(0).val = val
}

func (list *DLinkedList[T]) SetLast(val T) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	list.find(list.size - 1).val = val
}

func (list *DLinkedList[T]) Insert(idx int, val T) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	if idx < 0 || idx > list.size {
		panic(fmt.Sprintf("the insert index %d out of bound of [0, %d]", idx, list.size))
	}
	cur := new(Node[T])
	cur.val = val
	list.insertNode(idx, cur)
	list.size++
}

func (list *DLinkedList[T]) InsertAfter(preNode *Node[T], val T) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	if preNode == nil {
		panic("this preNode is nil")
	}
	newNode := new(Node[T])
	newNode.val = val
	newNode.next = preNode.next
	preNode.next = newNode
	newNode.prev = preNode
	// 添加位置为末尾时，更新tail
	if preNode == list.tail {
		list.tail = newNode
	}
}

func (list *DLinkedList[T]) Remove(idx int) T {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	list.boundCheck(idx)
	cur := list.find(idx)
	list.removeNode(cur)
	return cur.val
}

func (list *DLinkedList[T]) RemoveAll(condition Condition[T]) int {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	p, cnt := list.head, 0
	var nextNode *Node[T]
	for p != nil {
		nextNode = p.next
		if condition(p.val) {
			list.removeNode(p)
			cnt++
		}
		p = nextNode
	}
	return cnt
}
func (list *DLinkedList[T]) RemoveLeft(condition Condition[T], num int) int {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	p, cnt := list.head, 0
	var nextNode *Node[T]
	for p != nil && cnt < num {
		nextNode = p.next
		if condition(p.val) {
			list.removeNode(p)
			cnt++
		}
		p = nextNode
	}
	return cnt
}
func (list *DLinkedList[T]) RemoveRight(condition Condition[T], num int) int {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	p, cnt := list.tail, 0
	var nextNode *Node[T]
	for p != nil && cnt < num {
		nextNode = p.prev
		if condition(p.val) {
			list.removeNode(p)
			cnt++
		}
		p = nextNode
	}
	return cnt
}

func (list *DLinkedList[T]) Contains(condition Condition[T]) bool {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	res := false
	consumer := func(i int, val T) bool {
		var isMeet = condition(val) // 是否满足条件
		if isMeet {
			res = true
			return false
		}
		return true
	}
	list.ForEach(consumer)
	return res
}

func (list *DLinkedList[T]) Range(start int, stop int) []T {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	if start < 0 || start >= list.size {
		panic(fmt.Sprintf("the start index %d out of bound", start))
	}
	if stop < start || start > list.size {
		panic(fmt.Sprintf("the stop index %d out of bound", stop))
	}
	size := stop - start
	result := make([]T, size)
	var p = list.find(start)
	for i := 0; i < size; i++ {
		result[i] = p.val
		p = p.next
	}
	return result
}

func (list *DLinkedList[T]) ForEach(consumer Consumer[T]) {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	p, i := list.head, 0
	for p != nil {
		if !consumer(i, p.val) {
			break
		}
		p = p.next
		i++
	}
}

func (list *DLinkedList[T]) LPush(val T) {
	list.Insert(0, val)
}

func (list *DLinkedList[T]) RPush(val T) {
	list.Insert(list.size, val)
}

func (list *DLinkedList[T]) LPop() T {
	return list.Remove(0)
}

func (list *DLinkedList[T]) RPop() T {
	return list.Remove(list.size - 1)
}

func (list *DLinkedList[T]) boundCheck(idx int) {
	if idx < 0 || idx >= list.size {
		panic(fmt.Sprintf("the index %d out of bound of [0, %d]", idx, list.size-1))
	}
}

// 寻找指定下标的结点，返回其指针
func (list *DLinkedList[T]) find(idx int) (p *Node[T]) {
	if idx < list.size/2 {
		p = list.head
		for i := 0; i < idx; i++ {
			p = p.next
		}
	} else {
		p = list.tail
		for i := 0; i < list.size-1-idx; i++ {
			p = p.prev
		}
	}
	return p
}

// 指定下标插入一个结点
func (list *DLinkedList[T]) insertNode(idx int, cur *Node[T]) {
	if list.size == 0 {
		// 空链表
		list.head = cur
		list.tail = cur
		return
	}
	if idx == 0 {
		// 插入链头
		list.head.prev = cur
		cur.next = list.head
		list.head = cur
	} else if idx == list.size {
		// 插入链尾
		list.tail.next = cur
		cur.prev = list.tail
		list.tail = cur
	} else {
		// 插入链中
		var p = list.find(idx)
		p.prev.next = cur
		cur.prev = p.prev
		cur.next = p
		p.prev = cur
	}
}

// 删除指定结点
func (list *DLinkedList[T]) removeNode(cur *Node[T]) {
	if cur == list.head && cur == list.tail {
		// 即是头结点又是尾结点
		list.head = nil
		list.tail = nil
	} else if cur == list.head {
		// 当前结点是头结点
		list.head = cur.next
		list.head.prev = nil
	} else if cur == list.tail {
		// 当前结点是尾结点
		list.tail = cur.prev
		list.tail.next = nil
	} else {
		// 其他结点
		cur.prev.next = cur.next
		cur.next.prev = cur.prev
	}
	// 指针置空，方便垃圾回收
	cur.next = nil
	cur.prev = nil
	list.size--
}

func (list *DLinkedList[T]) String() string {
	vals := make([]string, list.size)
	consumer := func(i int, val T) bool {
		vals[i] = fmt.Sprintf("%v", val)
		return true
	}
	list.ForEach(consumer)
	return "[" + strings.Join(vals, ", ") + "]"
}

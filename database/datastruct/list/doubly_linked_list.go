package list

import (
	"fmt"
	"strings"
)

// 结点定义
type node[T any] struct {
	val  T        // 结点值
	prev *node[T] // 指向前一个节点
	next *node[T] // 指向后一个节点
}

// DLinkedList 链表定义
type DLinkedList[T any] struct {
	head *node[T] // 指向第一个结点
	tail *node[T] // 指向最后一个结点
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
	var curr = new(node[T])
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
	cur := new(node[T])
	cur.val = val
	list.insertNode(idx, cur)
	list.size++
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
	var nextNode *node[T]
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
func (list *DLinkedList[T]) RemoveLeft(condition Condition[T], count int) int {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	p, cnt := list.head, 0
	var nextNode *node[T]
	for p != nil && cnt < count {
		nextNode = p.next
		if condition(p.val) {
			list.removeNode(p)
			cnt++
		}
		p = nextNode
	}
	return cnt
}
func (list *DLinkedList[T]) RemoveRight(condition Condition[T], count int) int {
	if list == nil {
		panic("this DLinkedList is nil.")
	}
	p, cnt := list.tail, 0
	var nextNode *node[T]
	for p != nil && cnt < count {
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
		var hasNext = consumer(i, p.val) // 消费，并判断是否继续
		if !hasNext {
			break
		}
		p = p.next
		i++
	}
}

func (list *DLinkedList[T]) boundCheck(idx int) {
	if idx < 0 || idx >= list.size {
		panic(fmt.Sprintf("the index %d out of bound of [0, %d]", idx, list.size-1))
	}
}

// 寻找指定下标的结点，返回其指针
func (list *DLinkedList[T]) find(idx int) (p *node[T]) {
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
func (list *DLinkedList[T]) insertNode(idx int, cur *node[T]) {
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
func (list *DLinkedList[T]) removeNode(cur *node[T]) {
	if cur.prev == nil {
		list.head = cur.next
		list.head.prev = nil
	} else if cur.next == nil {
		list.tail = cur.prev
		list.tail.next = nil
	} else {
		cur.prev.next = cur.next
		cur.next.prev = cur.prev
	}
	// 指针置空，方便垃圾回收
	cur.next = nil
	cur.prev = nil
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

package list

import (
	"fmt"
)

const pageSize = 3 // page的大小, 必须设置为偶数

type Page[T any] []T // T的切片

// QuickList 本质上是个双端链表，其结点为page[T]类型
type QuickList[T any] struct {
	data *DLinkedList[Page[T]] // Page[T]类型的双端链表
	size int                   // T元素的个数
}

func MakeQuickList[T any]() *QuickList[T] {
	return &QuickList[T]{
		data: MakeDLinkedList[Page[T]](),
	}
}

func (list *QuickList[T]) Len() int {
	if list == nil {
		panic("this QuickList is nil.")
	}
	return list.size
}

func (list *QuickList[T]) Add(val T) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.size++
	if list.data.Len() == 0 {
		// 表为空时
		newPage := make([]T, 0, pageSize)
		newPage = append(newPage, val)
		list.data.Add(newPage)
		return
	}
	lastPage := list.data.GetLast()
	if len(lastPage) == cap(lastPage) {
		// lastPage已满，新建page
		newPage := make([]T, 0, pageSize)
		newPage = append(newPage, val)
		list.data.Add(newPage)
		return
	}
	lastPage = append(lastPage, val)
	list.data.SetLast(lastPage)
}

func (list *QuickList[T]) Get(idx int) (val T) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.boundCheck(idx)
	iter := list.find(idx)
	return iter.get()
}

func (list *QuickList[T]) Set(idx int, val T) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	iter := list.find(idx)
	iter.set(val)
}

func (list *QuickList[T]) Insert(idx int, val T) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	if idx == list.size {
		list.Add(val) // 添加到末尾
		return
	}
	iter := list.find(idx)
	page := iter.node.val
	if len(page) < pageSize {
		// 当前页未满
		offset := iter.idx
		page = append(page[:offset+1], page[offset:]...) // 空出page[idx]的位置
		page[iter.idx] = val
		iter.node.val = page
		list.size++
		return
	}
	// insert into a full Page may cause memory copy, so we split a full Page into two half pages
	// 当前页已满，将当前页均分为两个页
	var nextPage Page[T]
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]
	if iter.idx < len(page) {
		offset := iter.idx
		page = append(page[:offset+1], page[offset:]...) // 空出page[idx]的位置
		page[iter.idx] = val
	} else {
		offset := iter.idx - pageSize/2
		nextPage = append(nextPage[:offset+1], nextPage[offset:]...) // 空出page[idx]的位置
		nextPage[offset] = val
	}
	iter.node.val = page
	// 将nextPage插入到iter.node之后
	list.data.insertAfter(iter.node, nextPage)
	list.size++
}

func (list *QuickList[T]) Remove(idx int) T {
	iter := list.find(idx)
	return iter.remove()
}

//RemoveAll(condition Condition[T]) int              //  移除所有满足条件的元素，并返回其个数
//RemoveLeft(condition Condition[T], count int) int  // 从左到右移除指定数量的元素，并返回实际移除的个数
//RemoveRight(condition Condition[T], count int) int // 从右到左移除指定数量的元素，并返回实际移除的个数

func (list *QuickList[T]) RemoveAll(condition Condition[T]) int {
	if list == nil {
		panic("this QuickList is nil.")
	}
	if list.size == 0 {
		return 0
	}
	count := 0
	iter := list.find(0)
	if !iter.outEnd() {
		if condition(iter.get()) {
			iter.remove()
			count++
		} else {
			iter.next()
		}
	}
	return count
}

func (list *QuickList[T]) ForEach(consumer Consumer[T]) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	if list.size == 0 {
		return
	}
	idx := 0
	iter := list.find(0)
	for {
		if !consumer(idx, iter.get()) {
			break
		}
		if !iter.next() {
			break
		}
		idx++
	}
}

func (list *QuickList[T]) boundCheck(idx int) {
	if idx < 0 || idx >= list.size {
		panic(fmt.Sprintf("the index %d out of bound of [0, %d]", idx, list.size-1))
	}
}

// 返回一个iterator用于定位元素
func (list *QuickList[T]) find(idx int) *iterator[T] {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.boundCheck(idx)
	var curNode *Node[Page[T]]
	var offset int
	if idx < list.size/2 {
		// 从头部开始寻找
		curNode = list.data.head
		offset = 0
		for {
			curPage := curNode.val
			if offset+len(curPage) > idx {
				break
			}
			offset += len(curPage)
			curNode = curNode.next
		}
	} else {
		// 从尾部开始寻找
		curNode = list.data.tail
		offset = list.size
		for {
			curPage := curNode.val
			offset -= len(curPage)
			if offset <= idx {
				break
			}
			curNode = curNode.prev
		}
	}
	return &iterator[T]{
		list: list,
		node: curNode,
		idx:  idx - offset,
	}
}

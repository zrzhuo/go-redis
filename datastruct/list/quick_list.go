package list

import (
	"fmt"
)

const pageSize = 32 // page的大小, 必须设置为偶数

type Page[T any] []T // T的切片

func NewPage[T any]() Page[T] {
	return make(Page[T], 0, pageSize)
}

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
		newPage := NewPage[T]()
		newPage = append(newPage, val)
		list.data.Add(newPage)
		return
	}
	lastPage := list.data.GetLast()
	if len(lastPage) == cap(lastPage) {
		// lastPage已满，新建page
		newPage := NewPage[T]()
		newPage = append(newPage, val)
		list.data.Add(newPage)
	} else {
		// lastPage未满
		lastPage = append(lastPage, val)
		list.data.SetLast(lastPage)
	}
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
	list.boundCheck(idx)
	iter := list.find(idx)
	iter.set(val)
}

func (list *QuickList[T]) Insert(idx int, val T) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	if idx < 0 || idx > list.size {
		panic(fmt.Sprintf("the insert index %d out of bound of [0, %d]", idx, list.size))
	}
	if idx == list.size {
		list.Add(val) // 添加到末尾
		return
	}
	// 0 <= idx < list.size
	iter := list.find(idx)
	page := iter.node.val
	if len(page) < pageSize {
		// 当前页未满
		i := iter.idx
		page = append(page[:i+1], page[i:]...) // 空出page[i]的位置
		page[i] = val
		iter.node.val = page
		list.size++
		return
	}
	// 当前页已满，将当前页均分为两个页 (insert into a full Page may cause memory copy)
	nextPage := NewPage[T]()
	nextPage = append(nextPage, page[pageSize/2:]...)
	page = page[:pageSize/2]
	if iter.idx < len(page) {
		i := iter.idx
		page = append(page[:i+1], page[i:]...) // 空出page[i]的位置
		page[i] = val
	} else {
		i := iter.idx - pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...) // 空出page[i]的位置
		nextPage[i] = val
	}
	iter.node.val = page
	list.data.InsertAfter(iter.node, nextPage) // 将nextPage插入到iter.node之后
	list.size++
}

func (list *QuickList[T]) Remove(idx int) T {
	list.boundCheck(idx)
	iter := list.find(idx)
	return iter.remove()
}

func (list *QuickList[T]) RemoveAll(condition Condition[T]) int {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.boundCheck(0)
	iter, count := list.find(0), 0
	for !iter.outEnd() {
		if condition(iter.get()) {
			iter.remove() // 移除并next
			count++
		} else {
			iter.next()
		}
	}
	return count
}

func (list *QuickList[T]) RemoveFromLeft(condition Condition[T], num int) int {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.boundCheck(0)
	iter, count := list.find(0), 0
	for !iter.outEnd() && count < num {
		if condition(iter.get()) {
			iter.remove() // 移除并next
			count++
		} else {
			iter.next()
		}
	}
	return count

}

func (list *QuickList[T]) RemoveFromRight(condition Condition[T], num int) int {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.boundCheck(list.size - 1)
	iter, count := list.find(list.size-1), 0
	for !iter.outBegin() && count < num {
		if condition(iter.get()) {
			iter.remove() // 移除并next
			iter.prev()   // 再prev一下
			count++
		} else {
			iter.prev()
		}
	}
	return count
}

func (list *QuickList[T]) Contains(condition Condition[T]) bool {
	if list == nil {
		panic("this QuickList is nil.")
	}
	list.boundCheck(0)
	iter := list.find(0)
	for !iter.outEnd() {
		if condition(iter.get()) {
			return true
		}
		iter.next()
	}
	return false
}

func (list *QuickList[T]) Range(start int, stop int) []T {
	if list == nil {
		panic("this QuickList is nil.")
	}
	if start < 0 || start >= list.size {
		panic(fmt.Sprintf("the start index %d out of bound", start))
	}
	if stop < start || start > list.size {
		panic(fmt.Sprintf("the stop index %d out of bound", stop))
	}
	size := stop - start
	result := make([]T, size)
	iter := list.find(start)
	for i := 0; i < size; i++ {
		result[i] = iter.get()
		iter.next()
	}
	return result
}

func (list *QuickList[T]) ForEach(consumer Consumer[T]) {
	if list == nil {
		panic("this QuickList is nil.")
	}
	iter, idx := list.find(0), 0
	for !iter.outEnd() {
		if !consumer(idx, iter.get()) {
			break
		}
		iter.next()
		idx++
	}
}

func (list *QuickList[T]) LPush(val T) {
	list.Insert(0, val)
}

func (list *QuickList[T]) RPush(val T) {
	list.Insert(list.size, val)
}

func (list *QuickList[T]) LPop() T {
	return list.Remove(0)
}

func (list *QuickList[T]) RPop() T {
	return list.Remove(list.size - 1)
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

func (list *QuickList[T]) boundCheck(idx int) {
	if idx < 0 || idx >= list.size {
		panic(fmt.Sprintf("the index %d out of bound of [0, %d]", idx, list.size-1))
	}
}

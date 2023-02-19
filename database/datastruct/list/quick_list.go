package list

import (
	"fmt"
)

const pageSize = 3 // page的大小

type page[T any] []T // T的切片

// QuickList 本质上是个双端链表，其结点为page[T]类型
type QuickList[T any] struct {
	data *DLinkedList[page[T]] // page[T]类型的双端链表
	size int                   // T元素的个数
}

func MakeQuickList[T any]() *QuickList[T] {
	return &QuickList[T]{
		data: MakeDLinkedList[page[T]](),
	}
}

func (list *QuickList[T]) Len() int {
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
	list.boundCheck(idx)
	iter := list.find(idx)
	return iter.get()
}

func (list *QuickList[T]) Set(index int, val T) {
	iter := list.find(index)
	iter.set(val)
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
	var nowPage page[T]
	var offset int
	if idx < list.size/2 {
		// 从头部开始寻找
		curr := list.data.head
		offset = 0
		for {
			nowPage = curr.val
			if offset+len(nowPage) > idx {
				break
			}
			offset += len(nowPage)
			curr = curr.next
		}
	} else {
		// 从尾部开始寻找
		curr := list.data.tail
		offset = list.size
		for {
			nowPage = curr.val
			offset -= len(nowPage)
			if offset <= idx {
				break
			}
			curr = curr.prev
		}
	}
	return &iterator[T]{
		list:   list,
		page:   nowPage,
		offset: idx - offset,
	}
}

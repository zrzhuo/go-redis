package list

// 用于在QuickList中定位元素
type iterator[T any] struct {
	list   *QuickList[T] // 目标元素所在的快表
	page   page[T]       // 目标元素所在的页
	offset int           // 在当且页的偏移量
}

func (iter *iterator[T]) get() T {
	return iter.page[iter.offset]
}

func (iter *iterator[T]) set(val T) {
	iter.page[iter.offset] = val
}

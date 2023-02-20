package list

// 用于在QuickList中定位元素
type iterator[T any] struct {
	list *QuickList[T]  // 目标元素所在的快表
	node *Node[Page[T]] // 目标元素所在的页
	idx  int            // 目标元素的页内下标
}

func (iter *iterator[T]) get() T {
	page := iter.node.val
	return page[iter.idx]
}

func (iter *iterator[T]) set(val T) {
	page := iter.node.val
	page[iter.idx] = val
}

// remove 移除当前元素，并且使迭代器指向下一个元素
func (iter *iterator[T]) remove() T {
	page := iter.node.val
	val := page[iter.idx]
	page = append(page[:iter.idx], page[iter.idx+1:]...) // 将目标元素从page中剔除
	if len(page) > 0 {
		// 当前页不空
		iter.node.val = page
		// 移除的元素是本页的最末尾元素
		if iter.idx == len(page) {
			if iter.node != iter.list.data.tail {
				// 当前页不是最后一页
				iter.node = iter.node.next
				iter.idx = 0
			} else {
				// 当前页是最后一页，什么都不用做，idx为末页长度，即迭代器OutEnd
			}
		}
	} else {
		// 当且页已空
		if iter.node != iter.list.data.tail {
			// 当前页不是最后一页
			nextNode := iter.node.next
			iter.list.data.removeNode(iter.node) // 移除本页
			iter.node = nextNode
			iter.idx = 0
		} else {
			// 当前页是最后一页，此时将迭代器OutEnd
			iter.list.data.removeNode(iter.node)
			iter.node = iter.list.data.tail
			iter.idx = len(iter.list.data.tail.val)
		}
	}
	iter.list.size--
	return val
}

// next next并返回是否仍处于合法范围，即未OutEnd
func (iter *iterator[T]) next() bool {
	page := iter.node.val
	// 未超出当且页的返回
	if iter.idx < len(page)-1 {
		iter.idx++
		return true
	}
	// 已超出当且页，移动到一下页
	if iter.node == iter.list.data.tail {
		// 已位于最后一页
		iter.idx = len(page)
		return false
	}
	iter.node = iter.node.next
	iter.idx = 0
	return true
}

// prev prev并返回是否仍处于合法范围，即未OutBegin
func (iter *iterator[T]) prev() bool {
	// 未超出当前页范围
	if iter.idx > 0 {
		iter.idx--
		return true
	}
	// 已超出当前页范围，移动到上一页
	if iter.node == iter.list.data.head {
		// 已位于第一页
		iter.idx = -1
		return false
	}
	iter.node = iter.node.prev
	iter.idx = len(iter.node.val) - 1
	return true
}

// outEnd 判断迭代器是否超出尾界
func (iter *iterator[T]) outEnd() bool {
	if iter.list.size == 0 {
		return true
	}
	// 位于最末页，且idx为页长
	if iter.node == iter.list.data.tail && iter.idx == len(iter.node.val) {
		return true
	}
	return false
}

// outBegin 判断迭代器是否超出头界
func (iter *iterator[T]) outBegin() bool {
	if iter.list.size == 0 {
		return true
	}
	// 位于第一页，且idx为-1
	if iter.node == iter.list.data.head && iter.idx == -1 {
		return true
	}
	return false
}

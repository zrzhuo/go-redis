package sync

import (
	"go-redis/utils/fnv"
	"sort"
	"sync"
)

// Locker 的本质是一组mutex
type Locker struct {
	table []*sync.RWMutex
}

func MakeLocker(size int) *Locker {
	locker := &Locker{
		table: make([]*sync.RWMutex, size),
	}
	for i := 0; i < size; i++ {
		locker.table[i] = &sync.RWMutex{}
	}
	return locker
}

// 计算指定key应对应的锁的下标
func (locker *Locker) computeIndex(key string) uint32 {
	hashcode := fnv.Fnv32(key) // fnv32计算hash值
	size := uint32(len(locker.table))
	return (size - 1) & hashcode
}

func (locker *Locker) getMutex(key string) *sync.RWMutex {
	idx := locker.computeIndex(key)
	return locker.table[idx]
}

/* ---- Single Lock ----- */

func (locker *Locker) Lock(key string) {
	locker.getMutex(key).Lock()
}

func (locker *Locker) UnLock(key string) {
	locker.getMutex(key).Unlock()
}

func (locker *Locker) RLock(key string) {
	locker.getMutex(key).RLock()
}

func (locker *Locker) RUnLock(key string) {
	locker.getMutex(key).RUnlock()
}

/* ---- Batch Lock ----- */

func (locker *Locker) Locks(keys ...string) {
	indices := locker.toIndices(keys, false) // 正序上锁
	for _, index := range indices {
		locker.table[index].Lock()
	}
}

func (locker *Locker) UnLocks(keys ...string) {
	indices := locker.toIndices(keys, true) // 反序解锁
	for _, index := range indices {
		locker.table[index].Unlock()
	}
}

func (locker *Locker) RLocks(keys ...string) {
	indices := locker.toIndices(keys, false) // 正序上锁
	for _, index := range indices {
		locker.table[index].RLock()
	}
}

func (locker *Locker) RUnLocks(keys ...string) {
	indices := locker.toIndices(keys, true) // 反序解锁
	for _, index := range indices {
		locker.table[index].RUnlock()
	}
}

/* ---- Lock Keys ----- */

// LockKeys 给定一组write key和一组read key，按序进行上锁
func (locker *Locker) LockKeys(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locker.toIndices(keys, false)
	writeSet := make(map[uint32]bool)
	for _, key := range writeKeys {
		idx := locker.computeIndex(key)
		writeSet[idx] = true
	}
	for _, idx := range indices {
		_, isWrite := writeSet[idx]
		if isWrite {
			locker.table[idx].Lock() // write key
		} else {
			locker.table[idx].RLock() // read key
		}
	}
}

// UnLockKeys 给定一组write key和一组read key，按序进行解锁
func (locker *Locker) UnLockKeys(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locker.toIndices(keys, true)
	writeSet := make(map[uint32]bool)
	for _, key := range writeKeys {
		idx := locker.computeIndex(key)
		writeSet[idx] = true
	}
	for _, idx := range indices {
		_, isWrite := writeSet[idx]
		if isWrite {
			locker.table[idx].Unlock() // write key
		} else {
			locker.table[idx].RUnlock() // read key
		}
	}
}

// 根据给定的keys，计算涉及到的所有index，并按指定序返回
func (locker *Locker) toIndices(keys []string, reverse bool) []uint32 {
	idxMap := make(map[uint32]bool)
	for _, key := range keys {
		idx := locker.computeIndex(key)
		idxMap[idx] = true
	}
	indices := make([]uint32, len(idxMap))
	i := 0
	for idx := range idxMap {
		indices[i] = idx
		i++
	}
	// 排序，实现固定顺序上锁，避免因争夺锁而造成死锁
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

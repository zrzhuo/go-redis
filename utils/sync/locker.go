package sync

import (
	"go-redis/utils/fnv"
	"sort"
	"sync"
)

type Locker struct {
	table []*sync.RWMutex
}

func MakeLocker(tableSize int) *Locker {
	locks := &Locker{
		table: make([]*sync.RWMutex, tableSize),
	}
	for i := 0; i < tableSize; i++ {
		locks.table[i] = &sync.RWMutex{}
	}
	return locks
}

func (locker *Locker) spread(hashCode uint32) uint32 {
	if locker == nil {
		panic("locker is nil")
	}
	tableSize := uint32(len(locker.table))
	return (tableSize - 1) & hashCode
}

/* ---- Single Lock ----- */

func (locker *Locker) Lock(key string) {
	idx := locker.spread(fnv.Fnv32(key))
	locker.table[idx].Lock()
}

func (locker *Locker) UnLock(key string) {
	idx := locker.spread(fnv.Fnv32(key))
	locker.table[idx].Unlock()
}

func (locker *Locker) RLock(key string) {
	idx := locker.spread(fnv.Fnv32(key))
	locker.table[idx].RLock()
}

func (locker *Locker) RUnLock(key string) {
	idx := locker.spread(fnv.Fnv32(key))
	locker.table[idx].RUnlock()
}

/* ---- Batch Lock ----- */

func (locker *Locker) Locks(keys ...string) {
	indices := locker.toIndices(keys, false)
	for _, index := range indices {
		locker.table[index].Lock()
	}
}

func (locker *Locker) UnLocks(keys ...string) {
	indices := locker.toIndices(keys, true)
	for _, index := range indices {
		locker.table[index].Unlock()
	}
}

func (locker *Locker) RLocks(keys ...string) {
	indices := locker.toIndices(keys, false)
	for _, index := range indices {
		locker.table[index].RLock()
	}
}

func (locker *Locker) RUnLocks(keys ...string) {
	indices := locker.toIndices(keys, true)
	for _, index := range indices {
		locker.table[index].RUnlock()
	}
}

/* ---- Lock Keys ----- */

func (locker *Locker) LockKeys(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locker.toIndices(keys, false)
	writeSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		idx := locker.spread(fnv.Fnv32(key))
		writeSet[idx] = struct{}{}
	}
	for _, idx := range indices {
		if _, isWrite := writeSet[idx]; isWrite {
			locker.table[idx].Lock()
		} else {
			locker.table[idx].RLock()
		}
	}
}

func (locker *Locker) UnLockKeys(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locker.toIndices(keys, true)
	writeSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		idx := locker.spread(fnv.Fnv32(key))
		writeSet[idx] = struct{}{}
	}
	for _, idx := range indices {
		if _, isWrite := writeSet[idx]; isWrite {
			locker.table[idx].Unlock()
		} else {
			locker.table[idx].RUnlock()
		}
	}
}

// 根据给定的keys，计算涉及到的所有index，并按指定序返回
func (locker *Locker) toIndices(keys []string, reverse bool) []uint32 {
	idxMap := make(map[uint32]bool)
	for _, key := range keys {
		idx := locker.spread(fnv.Fnv32(key))
		idxMap[idx] = true
	}
	indices := make([]uint32, 0, len(idxMap))
	for idx := range idxMap {
		indices = append(indices, idx)
	}
	sort.Slice(indices, func(i, j int) bool {
		if !reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

package lock

import (
	"go-redis/utils/fnv"
	"sort"
	"sync"
)

// Locks provides rw locks for key
type Locks struct {
	table []*sync.RWMutex
}

func MakeLocks(tableSize int) *Locks {
	locks := &Locks{
		table: make([]*sync.RWMutex, tableSize),
	}
	for i := 0; i < tableSize; i++ {
		locks.table[i] = &sync.RWMutex{}
	}
	return locks
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("locks is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}

/* ---- Single Lock ----- */

func (locks *Locks) Lock(key string) {
	idx := locks.spread(fnv.Fnv32(key))
	locks.table[idx].Lock()
}

func (locks *Locks) UnLock(key string) {
	idx := locks.spread(fnv.Fnv32(key))
	locks.table[idx].Unlock()
}

func (locks *Locks) RLock(key string) {
	idx := locks.spread(fnv.Fnv32(key))
	locks.table[idx].RLock()
}

func (locks *Locks) RUnLock(key string) {
	idx := locks.spread(fnv.Fnv32(key))
	locks.table[idx].RUnlock()
}

/* ---- Batch Lock ----- */

func (locks *Locks) Locks(keys ...string) {
	indices := locks.toIndices(keys, false)
	for _, index := range indices {
		locks.table[index].Lock()
	}
}

func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toIndices(keys, true)
	for _, index := range indices {
		locks.table[index].Unlock()
	}
}

func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toIndices(keys, false)
	for _, index := range indices {
		locks.table[index].RLock()
	}
}

func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toIndices(keys, true)
	for _, index := range indices {
		locks.table[index].RUnlock()
	}
}

/* ---- RW Lock ----- */

// RWLocks locks write keys and read keys together. allow duplicate keys
func (locks *Locks) RWLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toIndices(keys, false)
	writeSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		idx := locks.spread(fnv.Fnv32(key))
		writeSet[idx] = struct{}{}
	}
	for _, idx := range indices {
		if _, isWrite := writeSet[idx]; isWrite {
			locks.table[idx].Lock()
		} else {
			locks.table[idx].RLock()
		}
	}
}

// RWUnLocks unlocks write keys and read keys together. allow duplicate keys
func (locks *Locks) RWUnLocks(writeKeys []string, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toIndices(keys, true)
	writeSet := make(map[uint32]struct{})
	for _, key := range writeKeys {
		idx := locks.spread(fnv.Fnv32(key))
		writeSet[idx] = struct{}{}
	}
	for _, idx := range indices {
		if _, isWrite := writeSet[idx]; isWrite {
			locks.table[idx].Unlock()
		} else {
			locks.table[idx].RUnlock()
		}
	}
}

// 根据给定的keys，计算涉及到的所有index，并按指定序返回
func (locks *Locks) toIndices(keys []string, reverse bool) []uint32 {
	idxMap := make(map[uint32]bool)
	for _, key := range keys {
		idx := locks.spread(fnv.Fnv32(key))
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

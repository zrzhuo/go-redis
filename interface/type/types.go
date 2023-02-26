package _type

import (
	"go-redis/datastruct/dict"
	"go-redis/datastruct/list"
	Set "go-redis/datastruct/set"
	ZSet "go-redis/datastruct/zset"
)

type CmdLine [][]byte
type Args [][]byte

const (
	StringType = 0
	ListType   = 1
	SetType    = 2
	ZSetType   = 3
	HashType   = 4
)

type Type interface {
	[]byte | list.QuickList[[]byte] | Set.SimpleSet[string] | ZSet.SortedSet[string] | dict.SimpleDict[string, []byte]
}

type Entity struct {
	Data any
}

func NewEntity(data any) *Entity {
	return &Entity{Data: data}
}

func (entity *Entity) GetType() int {
	if entity == nil {
		panic("this entity is nil.")
	}
	switch entity.Data.(type) {
	case []byte:
		return StringType
	case list.QuickList[[]byte]:
		return ListType
	case Set.SimpleSet[string]:
		return SetType
	case ZSet.SortedSet[string]:
		return ZSetType
	case dict.SimpleDict[string, []byte]:
		return HashType
	}
	return -1
}

package _type

import "go-redis/database/datastruct/list"

type CmdLine [][]byte
type Args [][]byte

type Type interface {
	string | list.QuickList[string]
}

// Entity stores data bound to a key, including a string, list, hash, set and so on
type Entity struct {
	Data any
}

func NewEntity(data any) *Entity {
	return &Entity{Data: data}
}

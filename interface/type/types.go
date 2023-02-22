package _type

import "go-redis/redis/datastruct/list"

type CmdLine [][]byte
type Args [][]byte

type Type interface {
	string | list.QuickList[string]
}

type Entity struct {
	Data any
}

func NewEntity(data any) *Entity {
	return &Entity{Data: data}
}

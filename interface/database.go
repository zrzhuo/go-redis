package _interface

import (
	_type "go-redis/interface/type"
	"time"
)

// DB is the interface for redis style storage engine
type DB interface {
	Exec(redisConn Connection, cmdLine _type.CmdLine) Reply
	AfterConnClose(redisConn Connection)
	Close()
}

// DBEngine is the embedding storage engine exposing more methods for complex application
type DBEngine interface {
	DB
	ExecWithLock(conn Connection, cmdLine _type.CmdLine) Reply
	ExecMulti(conn Connection, watching map[string]uint32, cmdLines []_type.CmdLine) Reply
	GetUndoLogs(dbIdx int, cmdLine _type.CmdLine) []_type.CmdLine
	ForEach(dbIdx int, cb func(key string, data *_type.Entity, expire *time.Time) bool)
	RWLocks(dbIdx int, writeKeys []string, readKeys []string)
	RWUnLocks(dbIdx int, writeKeys []string, readKeys []string)
	GetDBSize(dbIdx int) (int, int)
}

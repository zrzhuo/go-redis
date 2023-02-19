package database

import (
	"fmt"
	"go-redis/database/datastruct/dict"
	"go-redis/database/datastruct/lock"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
	"go-redis/utils/logger"
	"go-redis/utils/timewheel"
	"strings"
	"time"
)

const (
	dataSize   = 1 << 16
	ttlSize    = 1 << 10
	lockerSize = 1 << 10
)

type Database struct {
	idx     int                              // 数据库编号
	data    dict.Dict[string, *_type.Entity] // 数据
	ttl     dict.Dict[string, time.Time]     // 超时时间
	version dict.Dict[string, uint32]        // 版本
	locker  *lock.Locks                      // 锁
}

func MakeDatabase(idx int) *Database {
	database := &Database{
		idx:     idx,
		data:    dict.MakeConcurrent[string, *_type.Entity](dataSize),
		ttl:     dict.MakeConcurrent[string, time.Time](ttlSize),
		version: dict.MakeConcurrent[string, uint32](dataSize),
		locker:  lock.MakeLocks(lockerSize),
	}
	return database
}

func (db *Database) Execute(redisConn _interface.Connection, cmdLine _type.CmdLine) _interface.Reply {
	return db.execCommand(cmdLine)
}

func (db *Database) execCommand(cmdLine _type.CmdLine) _interface.Reply {
	cmdName := strings.ToLower(string(cmdLine[0])) // 获取命令
	cmd, ok := Commands[cmdName]
	// 是否存在该命令
	if !ok {
		return reply.MakeErrReply("unknown command '" + cmdName + "'")
	}
	// 参数个数是否满足要求
	if !checkArity(cmd.Arity, cmdLine) {
		return reply.MakeArgNumErrReply(cmdName)
	}
	args := _type.Args(cmdLine[1:]) // 获取参数
	writeKeys, readKeys := cmd.Prepare(args)
	//db.addVersion(writeKeys...)
	// 加锁
	db.RWLocks(writeKeys, readKeys)
	defer db.RWUnLocks(writeKeys, readKeys)
	return cmd.Execute(db, args)
}

// 检查参数个数是否满足要求
func checkArity(arity int, cmdLine _type.CmdLine) bool {
	argNum := len(cmdLine)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ---- Lock ----- */

// RWLocks lock keys for writing and reading
func (db *Database) RWLocks(writeKeys []string, readKeys []string) {
	db.locker.RWLocks(writeKeys, readKeys)
}

// RWUnLocks unlock keys for writing and reading
func (db *Database) RWUnLocks(writeKeys []string, readKeys []string) {
	db.locker.RWUnLocks(writeKeys, readKeys)
}

/* ---- Entity ----- */

func (db *Database) GetEntity(key string) (*_type.Entity, bool) {
	entity, ok := db.data.Get(key)
	if !ok {
		return nil, false // key不存在
	}
	if db.IsExpired(key) {
		return nil, false // key已过期
	}
	return entity, true
}

func (db *Database) PutEntity(key string, entity *_type.Entity) int {
	return db.data.Put(key, entity)
}

func (db *Database) PutIfExists(key string, entity *_type.Entity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *Database) PutIfAbsent(key string, entity *_type.Entity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *Database) Remove(key string) {
	db.data.Remove(key)
	db.ttl.Remove(key)
	taskKey := "expire:" + key
	timewheel.Cancel(taskKey)
}

func (db *Database) Removes(keys ...string) (count int) {
	count = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			count++
		}
	}
	return count
}

func (db *Database) Flush() {
	db.data.Clear()
	db.ttl.Clear()
	db.locker = lock.MakeLocks(lockerSize) // 重置锁
}

/* ---- Time To Live ---- */

func (db *Database) SetExpire(key string, expire time.Time) {
	db.ttl.Put(key, expire)
	taskKey := "expire:" + key
	// 创建定时任务
	timewheel.At(expire, taskKey, func() {
		keys := []string{key}
		// 上锁
		db.RWLocks(keys, nil)
		defer db.RWUnLocks(keys, nil)
		logger.Info(fmt.Sprintf("key '%s' expired", key))
		expireTime, ok := db.ttl.Get(key)
		if !ok {
			return
		}
		// 由于过期时间可能被更新，故需要再次检查是否过期
		isExpired := time.Now().After(expireTime)
		if isExpired {
			db.Remove(key)
		}
	})
}

func (db *Database) cancelExpire(key string) {
	db.ttl.Remove(key)
	taskKey := "expire:" + key
	timewheel.Cancel(taskKey)
}

func (db *Database) IsExpired(key string) bool {
	expire, ok := db.ttl.Get(key)
	if !ok {
		return false // 未设置过期时间
	}
	isExpired := time.Now().After(expire)
	if isExpired {
		db.Remove(key)
	}
	return isExpired
}

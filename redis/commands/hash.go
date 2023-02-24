package commands

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	reply2 "go-redis/resp/reply"
)

func init() {
	redis.RegisterCommand("HSet", execHSet, utils.WriteFirstKey, 4, redis.ReadWrite)
	//RegisterCommand("HSetNX", execHSetNX, writeFirstKey, undoHSet, 4, flagWrite)
	redis.RegisterCommand("HGet", execHGet, utils.ReadFirstKey, 3, redis.ReadOnly)
	//RegisterCommand("HExists", execHExists, readFirstKey, nil, 3, flagReadOnly)
	//RegisterCommand("HDel", execHDel, writeFirstKey, undoHDel, -3, flagWrite)
	//RegisterCommand("HLen", execHLen, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("HStrlen", execHStrlen, readFirstKey, nil, 3, flagReadOnly)
	//RegisterCommand("HMSet", execHMSet, writeFirstKey, undoHMSet, -4, flagWrite)
	//RegisterCommand("HMGet", execHMGet, readFirstKey, nil, -3, flagReadOnly)
	//RegisterCommand("HGet", execHGet, readFirstKey, nil, -3, flagReadOnly)
	//RegisterCommand("HKeys", execHKeys, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("HVals", execHVals, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("HGetAll", execHGetAll, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("HIncrBy", execHIncrBy, writeFirstKey, undoHIncr, 4, flagWrite)
	//RegisterCommand("HIncrByFloat", execHIncrByFloat, writeFirstKey, undoHIncr, 4, flagWrite)
	//RegisterCommand("HRandField", execHRandField, readFirstKey, nil, -2, flagReadOnly)
}

func execHSet(db *redis.Database, args _type.Args) _interface.Reply {
	key, field, value := string(args[0]), string(args[1]), args[2]
	dict, _, errReply := db.GetOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := dict.Put(field, value)
	//db.addAof(utils.ToCmdLine("hset", args...))
	return reply2.MakeIntReply(int64(result))
}

func execHGet(db *redis.Database, args _type.Args) _interface.Reply {
	key, field := string(args[0]), string(args[1])
	dict, errReply := db.GetDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return reply2.MakeNullBulkReply()
	}
	value, existed := dict.Get(field)
	if !existed {
		return reply2.MakeNullBulkReply()
	}
	return reply2.MakeBulkReply(value)
}

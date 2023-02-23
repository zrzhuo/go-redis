package commands

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	reply2 "go-redis/resp/reply"
	"strconv"
	"time"
)

func init() {
	redis.RegisterCommand("Exists", execExists, utils.ReadAllKeys, -2, redis.ReadOnly)
	redis.RegisterCommand("Del", execDel, utils.WriteAllKeys, -2, redis.ReadWrite)
	redis.RegisterCommand("Expire", execExpire, utils.WriteFirstKey, 3, redis.ReadWrite)
	redis.RegisterCommand("ExpireAt", execExpireAt, utils.WriteFirstKey, 3, redis.ReadWrite)
	//redis.RegisterCommand("ExpireTime", execExpireTime, readFirstKey, 2, flagReadOnly)
	//RegisterCommand("PExpire", execPExpire, writeFirstKey, undoExpire, 3, flagWrite)
	//RegisterCommand("PExpireAt", execPExpireAt, writeFirstKey, undoExpire, 3, flagWrite)
	//RegisterCommand("PExpireTime", execPExpireTime, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite)
	//RegisterCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly)
	//RegisterCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly)
	//RegisterCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly)
}

/*----- Execute: func(db *redis.Database, args _type.Args) _interface.Reply -----*/

func execExists(db *redis.Database, args _type.Args) _interface.Reply {
	var count int64 = 0
	for _, key := range args {
		_, existed := db.GetEntity(string(key))
		if existed {
			count++
		}
	}
	return reply2.MakeIntReply(count)
}

func execDel(db *redis.Database, args _type.Args) _interface.Reply {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	count := db.Removes(keys...)
	if count > 0 {
		db.ToAof(utils.ToCmdLine3("Del", args...))
	}
	return reply2.MakeIntReply(int64(count))
}

func execExpire(db *redis.Database, args _type.Args) _interface.Reply {
	num, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply2.MakeErrReply("illegal integer")
	}
	key := string(args[0])
	_, existed := db.GetEntity(key)
	if !existed {
		return reply2.MakeIntReply(0) // key不存在时返回0
	}
	expireAt := time.Now().Add(time.Duration(num) * time.Second)
	db.SetExpire(key, expireAt)
	//db.ToAof(aof.MakeExpireCmd(key, expireAt).Args)
	return reply2.MakeIntReply(1) // 设置成功时返回1
}

func execExpireAt(db *redis.Database, args _type.Args) _interface.Reply {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply2.MakeErrReply("illegal integer")
	}
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return reply2.MakeIntReply(0)
	}
	expireAt := time.Unix(ttl, 0)
	db.SetExpire(key, expireAt)
	//db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return reply2.MakeIntReply(1)
}

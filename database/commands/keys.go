package commands

import (
	"go-redis/database"
	"go-redis/database/commands/common"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/redis/resp/reply"
	"strconv"
	"time"
)

func init() {
	database.RegisterCommand("Exists", execExists, common.ReadAllKeys, -2, database.ReadOnly)
	database.RegisterCommand("Del", execDel, common.WriteAllKeys, -2, database.ReadWrite)
	database.RegisterCommand("Expire", execExpire, common.WriteFirstKey, 3, database.ReadWrite)
	database.RegisterCommand("ExpireAt", execExpireAt, common.WriteFirstKey, 3, database.ReadWrite)
	//database.RegisterCommand("ExpireTime", execExpireTime, readFirstKey, 2, flagReadOnly)
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

/*----- Execute: func(db *database.Database, args _type.Args) _interface.Reply -----*/

func execExists(db *database.Database, args _type.Args) _interface.Reply {
	var count int64 = 0
	for _, key := range args {
		_, existed := db.GetEntity(string(key))
		if existed {
			count++
		}
	}
	return Reply.MakeIntReply(count)
}

func execDel(db *database.Database, args _type.Args) _interface.Reply {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	count := db.Removes(keys...)
	if count > 0 {
		// aof
	}
	return Reply.MakeIntReply(int64(count))
}

func execExpire(db *database.Database, args _type.Args) _interface.Reply {
	num, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("illegal integer")
	}
	key := string(args[0])
	_, existed := db.GetEntity(key)
	if !existed {
		return Reply.MakeIntReply(0) // key不存在时返回0
	}
	expireAt := time.Now().Add(time.Duration(num) * time.Second)
	db.SetExpire(key, expireAt)
	//db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return Reply.MakeIntReply(1) // 设置成功时返回1
}

func execExpireAt(db *database.Database, args _type.Args) _interface.Reply {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("illegal integer")
	}
	key := string(args[0])
	_, exists := db.GetEntity(key)
	if !exists {
		return Reply.MakeIntReply(0)
	}
	expireAt := time.Unix(ttl, 0)
	db.SetExpire(key, expireAt)
	//db.addAof(aof.MakeExpireCmd(key, expireAt).Args)
	return Reply.MakeIntReply(1)
}

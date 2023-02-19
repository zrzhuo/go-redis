package commands

import (
	"go-redis/database"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
)

func init() {
	database.RegisterCommand("Del", delExec, writeAllKeys, -2, database.ReadWrite)
	//database.RegisterCommand("Expire", execExpire, writeFirstKey, 3, flagWrite)
	//database.RegisterCommand("ExpireAt", execExpireAt, writeFirstKey, 3, flagWrite)
	//database.RegisterCommand("ExpireTime", execExpireTime, readFirstKey, 2, flagReadOnly)
	//RegisterCommand("PExpire", execPExpire, writeFirstKey, undoExpire, 3, flagWrite)
	//RegisterCommand("PExpireAt", execPExpireAt, writeFirstKey, undoExpire, 3, flagWrite)
	//RegisterCommand("PExpireTime", execPExpireTime, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("TTL", execTTL, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("PTTL", execPTTL, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("Persist", execPersist, writeFirstKey, undoExpire, 2, flagWrite)
	//RegisterCommand("Exists", execExists, readAllKeys, nil, -2, flagReadOnly)
	//RegisterCommand("Type", execType, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly)
	//RegisterCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly)
	//RegisterCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly)
}

// type Execute func(db *database.Database, args _type.Args) _interface.Reply

func delExec(db *database.Database, args _type.Args) _interface.Reply {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	count := db.Removes(keys...)
	if count > 0 {
		// aof
	}
	return reply.MakeIntReply(int64(count))
}

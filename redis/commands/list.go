package commands

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	reply2 "go-redis/resp/reply"
	"strconv"
)

func init() {
	redis.RegisterCommand("LPush", execLPush, utils.WriteFirstKey, -3, redis.ReadWrite)
	redis.RegisterCommand("RPush", execRPush, utils.WriteFirstKey, -3, redis.ReadWrite)
	//RegisterCommand("LPushX", execLPushX, writeFirstKey, undoRPush, -3, flagWrite)
	//RegisterCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3, flagWrite)
	redis.RegisterCommand("LPop", execLPop, utils.WriteFirstKey, 2, redis.ReadWrite)
	redis.RegisterCommand("RPop", execRPop, utils.WriteFirstKey, 2, redis.ReadWrite)
	//RegisterCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3, flagWrite)
	//RegisterCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	redis.RegisterCommand("LLen", execLLen, utils.ReadFirstKey, 2, redis.ReadOnly)
	//RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3, flagReadOnly)
	//RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4, flagWrite)
	redis.RegisterCommand("LRange", execLRange, utils.ReadFirstKey, 4, redis.ReadOnly)
}

func execLPush(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, _, errReply := db.GetOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, val := range vals {
		list.Insert(0, val) // 按顺序插入表头
	}
	//db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply2.MakeIntReply(int64(list.Len()))
}

func execRPush(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, _, errReply := db.GetOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, val := range vals {
		list.Insert(list.Len(), val) // 按顺序插入表尾
	}
	//db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply2.MakeIntReply(int64(list.Len()))
}

func execLPop(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply2.MakeNullBulkReply()
	}
	val := list.Remove(0)
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	//db.addAof(utils.ToCmdLine3("lpop", args...))
	return reply2.MakeBulkReply(val)
}

func execRPop(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply2.MakeNullBulkReply()
	}
	val := list.Remove(list.Len() - 1)
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	//db.addAof(utils.ToCmdLine3("lpop", args...))
	return reply2.MakeBulkReply(val)
}

func execLLen(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply2.MakeIntReply(0)
	}
	size := list.Len()
	return reply2.MakeIntReply(int64(size))
}

func execLRange(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	first, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply2.MakeErrReply("start value is not an illegal integer")
	}
	second, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply2.MakeErrReply("stop value is not an illegal integer")
	}
	start, stop := int(first), int(second)
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply2.MakeEmptyMultiBulkReply()
	}
	size := list.Len()
	if start < 0 || start >= size {
		return reply2.MakeErrReply(fmt.Sprintf("the start index %d out of bound", start))
	}
	if stop < start || start > size {
		return reply2.MakeErrReply(fmt.Sprintf("the stop index %d out of bound", stop))
	}
	vals := list.Range(start, stop)
	return reply2.MakeMultiBulkReply(vals)
}

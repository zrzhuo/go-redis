package commands

import (
	"fmt"
	"go-redis/database"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
	"strconv"
)

func init() {
	database.RegisterCommand("LPush", execLPush, writeFirstKey, -3, database.ReadWrite)
	database.RegisterCommand("RPush", execRPush, writeFirstKey, -3, database.ReadWrite)
	//RegisterCommand("LPushX", execLPushX, writeFirstKey, undoRPush, -3, flagWrite)
	//RegisterCommand("RPushX", execRPushX, writeFirstKey, undoRPush, -3, flagWrite)
	database.RegisterCommand("LPop", execLPop, writeFirstKey, 2, database.ReadWrite)
	database.RegisterCommand("RPop", execRPop, writeFirstKey, 2, database.ReadWrite)
	//RegisterCommand("RPopLPush", execRPopLPush, prepareRPopLPush, undoRPopLPush, 3, flagWrite)
	//RegisterCommand("LRem", execLRem, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	database.RegisterCommand("LLen", execLLen, readFirstKey, 2, database.ReadOnly)
	//RegisterCommand("LIndex", execLIndex, readFirstKey, nil, 3, flagReadOnly)
	//RegisterCommand("LSet", execLSet, writeFirstKey, undoLSet, 4, flagWrite)
	database.RegisterCommand("LRange", execLRange, readFirstKey, 4, database.ReadOnly)
}

func execLPush(db *database.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, _, errReply := db.GetOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, val := range vals {
		list.Insert(0, val) // 按顺序插入表头
	}
	//db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

func execRPush(db *database.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, _, errReply := db.GetOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, val := range vals {
		list.Insert(list.Len(), val) // 按顺序插入表尾
	}
	//db.addAof(utils.ToCmdLine3("lpush", args...))
	return reply.MakeIntReply(int64(list.Len()))
}

func execLPop(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeNullBulkReply()
	}
	val := list.Remove(0)
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	//db.addAof(utils.ToCmdLine3("lpop", args...))
	return reply.MakeBulkReply(val)
}

func execRPop(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeNullBulkReply()
	}
	val := list.Remove(list.Len() - 1)
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	//db.addAof(utils.ToCmdLine3("lpop", args...))
	return reply.MakeBulkReply(val)
}

func execLLen(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeIntReply(0)
	}
	size := list.Len()
	return reply.MakeIntReply(int64(size))
}

func execLRange(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	first, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("start value is not an illegal integer")
	}
	second, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return reply.MakeErrReply("stop value is not an illegal integer")
	}
	start, stop := int(first), int(second)
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return reply.MakeEmptyMultiBulkReply()
	}
	size := list.Len()
	if start < 0 || start >= size {
		return reply.MakeErrReply(fmt.Sprintf("the start index %d out of bound", start))
	}
	if stop < start || start > size {
		return reply.MakeErrReply(fmt.Sprintf("the stop index %d out of bound", stop))
	}
	vals := list.Range(start, stop)
	return reply.MakeMultiBulkReply(vals)
}

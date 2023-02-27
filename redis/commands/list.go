package commands

import (
	"bytes"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"strconv"
)

func init() {
	redis.RegisterCommand("LPush", execLPush, utils.WriteFirstKey, -3, redis.ReadWrite)
	redis.RegisterCommand("RPush", execRPush, utils.WriteFirstKey, -3, redis.ReadWrite)
	redis.RegisterCommand("LPushX", execLPushX, utils.WriteFirstKey, -3, redis.ReadWrite)
	redis.RegisterCommand("RPushX", execRPushX, utils.WriteFirstKey, -3, redis.ReadWrite)
	redis.RegisterCommand("LPop", execLPop, utils.WriteFirstKey, 2, redis.ReadWrite)
	redis.RegisterCommand("RPop", execRPop, utils.WriteFirstKey, 2, redis.ReadWrite)
	redis.RegisterCommand("RPopLPush", execRPopLPush, utils.ReadTwoKeys, 3, redis.ReadWrite)
	redis.RegisterCommand("LLen", execLLen, utils.ReadFirstKey, 2, redis.ReadOnly)
	redis.RegisterCommand("LIndex", execLIndex, utils.ReadFirstKey, 3, redis.ReadOnly)
	redis.RegisterCommand("LSet", execLSet, utils.WriteFirstKey, 4, redis.ReadWrite)
	redis.RegisterCommand("LRem", execLRem, utils.WriteFirstKey, 4, redis.ReadWrite)
	redis.RegisterCommand("LRange", execLRange, utils.ReadFirstKey, 4, redis.ReadOnly)
}

func execLPush(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, _, errReply := db.GetOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, val := range vals {
		list.LPush(val) // 按顺序插入表头
	}
	db.ToAof(utils.ToCmd("LPush", args...))
	return Reply.MakeIntReply(int64(list.Len()))
}

func execRPush(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, _, errReply := db.GetOrInitList(key)
	if errReply != nil {
		return errReply
	}
	for _, val := range vals {
		list.RPush(val) // 按顺序插入表尾
	}
	db.ToAof(utils.ToCmd("RPush", args...))
	return Reply.MakeIntReply(int64(list.Len()))
}

func execLPushX(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeIntReply(0)
	}
	for _, val := range vals {
		list.LPush(val) // 按顺序插入表头
	}
	db.ToAof(utils.ToCmd("LPushX", args...))
	return Reply.MakeIntReply(int64(list.Len()))
}
func execRPushX(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeIntReply(0)
	}
	for _, val := range vals {
		list.RPush(val) // 按顺序插入表头
	}
	db.ToAof(utils.ToCmd("RPushX", args...))
	return Reply.MakeIntReply(int64(list.Len()))
}

func execLPop(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeNullBulkReply()
	}
	val := list.LPop()
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	db.ToAof(utils.ToCmd("LPop", args...))
	return Reply.MakeBulkReply(val)
}

func execRPop(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeNullBulkReply()
	}
	val := list.RPop()
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	db.ToAof(utils.ToCmd("RPop", args...))
	return Reply.MakeBulkReply(val)
}

func execRPopLPush(db *redis.Database, args _type.Args) _interface.Reply {
	srcKey, destKey := string(args[0]), string(args[1])
	srcList, errReply := db.GetList(srcKey)
	if errReply != nil {
		return errReply
	}
	if srcList == nil {
		return Reply.MakeNullBulkReply()
	}
	destList, _, errReply := db.GetOrInitList(destKey) // 初始化destList
	if errReply != nil {
		return errReply
	}
	val := srcList.RPop() // RPop
	destList.LPush(val)   // LPush
	if srcList.Len() == 0 {
		db.Remove(srcKey) // list已为空，移除该key
	}
	db.ToAof(utils.ToCmd("RPopLPush", args...))
	return Reply.MakeBulkReply(val)
}

func execLLen(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeIntReply(0)
	}
	size := list.Len()
	return Reply.MakeIntReply(int64(size))
}

func execLSet(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	idx, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("index value is not an illegal integer")
	}
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeErrReply("no such key")
	}
	// 解析index
	size, index := list.Len(), int(idx)
	if index >= size {
		return Reply.MakeErrReply("index out of range")
	} else if index < -size {
		return Reply.MakeErrReply("index out of range")
	} else if index < 0 {
		index = size + index
	}
	list.Set(index, args[2])
	return Reply.MakeOkReply()
}

func execLIndex(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	idx, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("index value is not an illegal integer")
	}
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeNullBulkReply()
	}
	size, index := list.Len(), int(idx)
	// 解析index
	if index >= size {
		return Reply.MakeNullBulkReply()
	} else if index < -size {
		return Reply.MakeNullBulkReply()
	} else if index < 0 {
		index = size + index
	}
	val := list.Get(index)
	return Reply.MakeBulkReply(val)
}

func execLRem(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	nun, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("index value is not an illegal integer")
	}
	target := args[2]
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeIntReply(0)
	}
	equals := func(val []byte) bool {
		return bytes.Equal(val, target)
	}
	var count int
	if nun > 0 {
		count = list.RemoveLeft(equals, int(nun))
	} else if nun < 0 {
		count = list.RemoveRight(equals, int(-nun))
	} else {
		count = list.RemoveAll(equals)
	}
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	if count > 0 {
		db.ToAof(utils.ToCmd("LRem", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execLRange(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	first, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("start value is not an illegal integer")
	}
	second, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("stop value is not an illegal integer")
	}
	start, stop := int(first), int(second)
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.MakeEmptyMultiBulkReply()
	}
	size := list.Len()
	// 解析start
	if start < -size {
		start = 0
	} else if start < 0 {
		start = size + start
	} else if start >= size {
		return Reply.MakeNullBulkReply()
	}
	// 解析stop
	if stop < -size {
		stop = 0
	} else if stop < 0 {
		stop = size + stop + 1
	} else if stop < size {
		stop = stop + 1
	} else {
		stop = size
	}
	// stop小于start
	if stop < start {
		stop = start
	}
	vals := list.Range(start, stop)
	return Reply.MakeArrayReply(vals)
}

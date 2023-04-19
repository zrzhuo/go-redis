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
	redis.RegisterCommand("LPush", execLPush, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("RPush", execRPush, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("LPushX", execLPushX, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("RPushX", execRPushX, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("LPop", execLPop, utils.WriteFirst, 2, redis.ReadWrite)
	redis.RegisterCommand("RPop", execRPop, utils.WriteFirst, 2, redis.ReadWrite)
	redis.RegisterCommand("RPopLPush", execRPopLPush, utils.ReadFirstTwo, 3, redis.ReadWrite)
	redis.RegisterCommand("LLen", execLLen, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("LIndex", execLIndex, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("LSet", execLSet, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("LRem", execLRem, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("LRange", execLRange, utils.ReadFirst, 4, redis.ReadOnly)
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
	db.ToAOF(utils.ToCmd("LPush", args...))
	return Reply.NewIntegerReply(int64(list.Len()))
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
	db.ToAOF(utils.ToCmd("RPush", args...))
	return Reply.NewIntegerReply(int64(list.Len()))
}

func execLPushX(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewIntegerReply(0)
	}
	for _, val := range vals {
		list.LPush(val) // 按顺序插入表头
	}
	db.ToAOF(utils.ToCmd("LPushX", args...))
	return Reply.NewIntegerReply(int64(list.Len()))
}
func execRPushX(db *redis.Database, args _type.Args) _interface.Reply {
	key, vals := string(args[0]), args[1:]
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewIntegerReply(0)
	}
	for _, val := range vals {
		list.RPush(val) // 按顺序插入表头
	}
	db.ToAOF(utils.ToCmd("RPushX", args...))
	return Reply.NewIntegerReply(int64(list.Len()))
}

func execLPop(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewNilBulkReply()
	}
	val := list.LPop()
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	db.ToAOF(utils.ToCmd("LPop", args...))
	return Reply.NewBulkReply(val)
}

func execRPop(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewNilBulkReply()
	}
	val := list.RPop()
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	db.ToAOF(utils.ToCmd("RPop", args...))
	return Reply.NewBulkReply(val)
}

func execRPopLPush(db *redis.Database, args _type.Args) _interface.Reply {
	srcKey, destKey := string(args[0]), string(args[1])
	srcList, errReply := db.GetList(srcKey)
	if errReply != nil {
		return errReply
	}
	if srcList == nil {
		return Reply.NewNilBulkReply()
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
	db.ToAOF(utils.ToCmd("RPopLPush", args...))
	return Reply.NewBulkReply(val)
}

func execLLen(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewIntegerReply(0)
	}
	size := list.Len()
	return Reply.NewIntegerReply(int64(size))
}

func execLSet(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	idx, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.StandardError("no such key")
	}
	// 解析index
	size, index := list.Len(), int(idx)
	if index >= size {
		return Reply.StandardError("value is not an integer or out of range")
	} else if index < -size {
		return Reply.StandardError("value is not an integer or out of range")
	} else if index < 0 {
		index = size + index
	}
	list.Set(index, args[2])
	return Reply.NewOkReply()
}

func execLIndex(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	idx, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewNilBulkReply()
	}
	size, index := list.Len(), int(idx)
	// 解析index
	if index >= size {
		return Reply.NewNilBulkReply()
	} else if index < -size {
		return Reply.NewNilBulkReply()
	} else if index < 0 {
		index = size + index
	}
	val := list.Get(index)
	return Reply.NewBulkReply(val)
}

func execLRem(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	num, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	target := args[2]
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewIntegerReply(0)
	}
	equals := func(val []byte) bool {
		return bytes.Equal(val, target)
	}
	var count int
	if num > 0 {
		count = list.RemoveFromLeft(equals, int(num))
	} else if num < 0 {
		count = list.RemoveFromRight(equals, int(-num))
	} else {
		count = list.RemoveAll(equals)
	}
	if list.Len() == 0 {
		db.Remove(key) // list已为空，移除该key
	}
	if count > 0 {
		db.ToAOF(utils.ToCmd("LRem", args...))
	}
	return Reply.NewIntegerReply(int64(count))
}

func execLRange(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	first, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	second, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	list, errReply := db.GetList(key)
	if errReply != nil {
		return errReply
	}
	if list == nil {
		return Reply.NewEmptyArrayReply()
	}
	left, right := utils.ParseRange(int(first), int(second), list.Len())
	if left < 0 {
		return Reply.NewNilBulkReply()
	}
	if right < list.Len() {
		right = right + 1
	}
	vals := list.Range(left, right)
	return Reply.NewArrayReply(vals)
}

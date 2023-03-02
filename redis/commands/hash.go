package commands

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"strconv"
)

func init() {
	redis.RegisterCommand("HSet", execHSet, utils.WriteFirst, -4, redis.ReadWrite)
	redis.RegisterCommand("HSetNX", execHSetNX, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("HGet", execHGet, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("HMGet", execHMGet, utils.ReadFirst, -3, redis.ReadOnly)
	redis.RegisterCommand("HKeys", execHKeys, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("HVals", execHVals, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("HGetAll", execHGetAll, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("HDel", execHDel, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("HLen", execHLen, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("HExists", execHExists, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("HStrlen", execHStrlen, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("HIncrBy", execHIncrBy, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("HIncrByFloat", execHIncrByFloat, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("HRandField", execHRandField, utils.ReadFirst, -2, redis.ReadOnly)
}

func execHSet(db *redis.Database, args _type.Args) _interface.Reply {
	length := len(args)
	if length%2 == 0 {
		return Reply.ArgNumError("HSet")
	}
	dict, _, errReply := db.GetOrInitDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	count := 0
	for i := 0; i < length/2; i++ {
		field, value := string(args[2*i+1]), args[2*i+2]
		count += dict.Put(field, value)
	}
	if count > 0 {
		db.ToAOF(utils.ToCmd("HSet", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execHSetNX(db *redis.Database, args _type.Args) _interface.Reply {
	key, field, value := string(args[0]), string(args[1]), args[2]
	dict, _, errReply := db.GetOrInitDict(key)
	if errReply != nil {
		return errReply
	}
	result := dict.PutIfAbsent(field, value)
	if result > 0 {
		db.ToAOF(utils.ToCmd("HSetNX", args...))
	}
	return Reply.MakeIntReply(int64(result))
}

func execHGet(db *redis.Database, args _type.Args) _interface.Reply {
	key, field := string(args[0]), string(args[1])
	dict, errReply := db.GetDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeNilBulkReply()
	}
	value, existed := dict.Get(field)
	if !existed {
		return Reply.MakeNilBulkReply()
	}
	return Reply.MakeBulkReply(value)
}

func execHMGet(db *redis.Database, args _type.Args) _interface.Reply {
	vals := make([][]byte, len(args)-1)
	dict, errReply := db.GetDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeArrayReply(vals)
	}
	for i, arg := range args[1:] {
		val, existed := dict.Get(string(arg))
		if !existed {
			continue
		}
		vals[i] = val
	}
	return Reply.MakeArrayReply(vals)
}

func execHKeys(db *redis.Database, args _type.Args) _interface.Reply {
	dict, errReply := db.GetDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeEmptyArrayReply()
	}
	fields := dict.Keys()
	return Reply.StringToArrayReply(fields...)
}

func execHVals(db *redis.Database, args _type.Args) _interface.Reply {
	dict, errReply := db.GetDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeEmptyArrayReply()
	}
	values := dict.Values()
	return Reply.MakeArrayReply(values)
}

func execHGetAll(db *redis.Database, args _type.Args) _interface.Reply {
	dict, errReply := db.GetDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeEmptyArrayReply()
	}
	length := dict.Len()
	result := make([][]byte, 2*length)
	i := 0
	consumer := func(key string, val []byte) bool {
		result[2*i] = []byte(key)
		result[2*i+1] = val
		i++
		return true
	}
	dict.ForEach(consumer)
	return Reply.MakeArrayReply(result)
}

func execHDel(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	dict, errReply := db.GetDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeIntReply(0)
	}
	count := 0
	for _, arg := range args[1:] {
		count += dict.Remove(string(arg))
	}
	if dict.Len() == 0 {
		db.Remove(key)
	}
	if count > 0 {
		db.ToAOF(utils.ToCmd("HDel", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execHLen(db *redis.Database, args _type.Args) _interface.Reply {
	dict, errReply := db.GetDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeIntReply(0)
	}
	return Reply.MakeIntReply(int64(dict.Len()))
}

func execHExists(db *redis.Database, args _type.Args) _interface.Reply {
	key, field := string(args[0]), string(args[1])
	dict, errReply := db.GetDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeIntReply(0)
	}
	existed := dict.ContainKey(field)
	if !existed {
		return Reply.MakeIntReply(0)
	}
	return Reply.MakeIntReply(1)
}

func execHStrlen(db *redis.Database, args _type.Args) _interface.Reply {
	key, field := string(args[0]), string(args[1])
	dict, errReply := db.GetDict(key)
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		return Reply.MakeIntReply(0)
	}
	val, existed := dict.Get(field)
	if !existed {
		return Reply.MakeIntReply(0)
	}
	return Reply.MakeIntReply(int64(len(val)))
}

func execHRandField(db *redis.Database, args _type.Args) _interface.Reply {
	length := len(args)
	if length > 3 {
		return Reply.SyntaxError()
	}
	dict, errReply := db.GetDict(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if dict == nil {
		switch length {
		case 1:
			return Reply.MakeNilBulkReply()
		case 2:
			return Reply.MakeEmptyArrayReply()
		case 3:
			return Reply.MakeEmptyArrayReply()
		}
	}
	switch length {
	case 1:
		keys := dict.RandomDistinctKeys(1)
		if len(keys) == 0 {
			return Reply.MakeNilBulkReply()
		}
		return Reply.MakeBulkReply([]byte(keys[0]))
	case 2:
		number, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return Reply.StandardError("value is not an integer or out of range")
		}
		count := int(number)
		if count >= 0 {
			fields := dict.RandomDistinctKeys(count)
			return Reply.StringToArrayReply(fields...)
		} else {
			fields := dict.RandomKeys(-count)
			return Reply.StringToArrayReply(fields...)
		}
	case 3:
		number, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return Reply.StandardError("value is not an integer or out of range")
		}
		count := int(number)
		if string(args[2]) != "withvalues" {
			return Reply.SyntaxError()
		}
		if count >= 0 {
			fields := dict.RandomDistinctKeys(count)
			result := make([][]byte, 2*len(fields))
			for i, field := range fields {
				value, _ := dict.Get(field)
				result[2*i] = []byte(field)
				result[2*i+1] = value
			}
			return Reply.MakeArrayReply(result)
		} else {
			result := make([][]byte, -2*count)
			fields := dict.RandomKeys(-count)
			for i, field := range fields {
				value, _ := dict.Get(field)
				result[2*i] = []byte(field)
				result[2*i+1] = value
			}
			return Reply.MakeArrayReply(result)
		}
	default:
		return Reply.SyntaxError()
	}
}

func execHIncrBy(db *redis.Database, args _type.Args) _interface.Reply {
	key, field := string(args[0]), string(args[1])
	increment, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	dict, _, errReply := db.GetOrInitDict(key) // key不存在时新建dict
	if errReply != nil {
		return errReply
	}
	value, existed := dict.Get(field)
	if !existed {
		dict.Put(field, args[2]) // 相当于0+increment
		db.ToAOF(utils.ToCmd("HIncrBy", args...))
		return Reply.MakeIntReply(increment)
	}
	oldVal, err := strconv.ParseInt(string(value), 10, 64)
	if err != nil {
		return Reply.StandardError("hash value is not an integer")
	}
	newVal := oldVal + increment
	dict.Put(field, []byte(strconv.FormatInt(newVal, 10)))
	db.ToAOF(utils.ToCmd("HIncrBy", args...))
	return Reply.MakeIntReply(newVal)
}

func execHIncrByFloat(db *redis.Database, args _type.Args) _interface.Reply {
	key, field := string(args[0]), string(args[1])
	increment, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return Reply.StandardError("value is not a valid float")
	}
	dict, _, errReply := db.GetOrInitDict(key) // key不存在时新建dict
	if errReply != nil {
		return errReply
	}
	value, existed := dict.Get(field)
	if !existed {
		dict.Put(field, args[2]) // 相当于0+increment
		db.ToAOF(utils.ToCmd("HIncrByFloat", args...))
		return Reply.MakeBulkReply(args[2])
	}
	oldVal, err := strconv.ParseFloat(string(value), 64)
	if err != nil {
		return Reply.StandardError("hash value is not an float")
	}
	newVal := oldVal + increment
	value = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))
	dict.Put(field, value)
	db.ToAOF(utils.ToCmd("HIncrByFloat", args...))
	return Reply.MakeBulkReply(value)
}

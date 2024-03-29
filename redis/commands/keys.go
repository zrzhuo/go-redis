package commands

import (
	Dict "go-redis/datastruct/dict"
	List "go-redis/datastruct/list"
	Set "go-redis/datastruct/set"
	"go-redis/datastruct/zset"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"strconv"
	"time"
)

func init() {
	redis.RegisterCommand("Exists", execExists, utils.ReadAll, -2, redis.ReadOnly)
	redis.RegisterCommand("Del", execDel, utils.WriteAll, -2, redis.ReadWrite)
	redis.RegisterCommand("Expire", execExpire, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("ExpireAt", execExpireAt, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("TTL", execTTL, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("ExpireTime", execExpireTime, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("PExpire", execPExpire, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("PExpireAt", execPExpireAt, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("PTTL", execPTTL, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("PExpireTime", execPExpireTime, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("Persist", execPersist, utils.WriteFirst, 2, redis.ReadWrite)
	redis.RegisterCommand("Type", execType, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("Rename", execRename, utils.WriteFirstReadSecond, 3, redis.ReadOnly)
	redis.RegisterCommand("RenameNx", execRenameNx, utils.WriteFirstReadSecond, 3, redis.ReadOnly)
	redis.RegisterCommand("Keys", execKeys, utils.WriteNilReadNil, 2, redis.ReadOnly)
}

func execExists(db *redis.Database, args _type.Args) _interface.Reply {
	var count int64 = 0
	for _, key := range args {
		_, existed := db.Get(string(key))
		if existed {
			count++
		}
	}
	return Reply.NewIntegerReply(count)
}

func execDel(db *redis.Database, args _type.Args) _interface.Reply {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	count := db.Removes(keys...)
	if count > 0 {
		db.ToAOF(utils.ToCmd("Del", args...))
	}
	return Reply.NewIntegerReply(int64(count))
}

func execExpire(db *redis.Database, args _type.Args) _interface.Reply {
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(0) // key不存在，返回0
	}
	ttl := time.Duration(ttlArg) * time.Second // 以秒为单位
	expireTime := time.Now().Add(ttl)
	db.SetExpire(key, expireTime)
	db.ToAOF(utils.ToExpireCmd(key, expireTime))
	return Reply.NewIntegerReply(1) // 设置成功，返回1
}

func execPExpire(db *redis.Database, args _type.Args) _interface.Reply {
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(0) // key不存在，返回0
	}
	ttl := time.Duration(ttlArg) * time.Millisecond // 以毫秒为单位
	expireTime := time.Now().Add(ttl)
	db.SetExpire(key, expireTime)
	db.ToAOF(utils.ToExpireCmd(key, expireTime))
	return Reply.NewIntegerReply(1) // 设置成功，返回1
}

func execExpireAt(db *redis.Database, args _type.Args) _interface.Reply {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(0) // key不存在，返回0
	}
	expireTime := time.Unix(ttl, 0) // 以秒为单位的unix时间
	db.SetExpire(key, expireTime)
	db.ToAOF(utils.ToExpireCmd(key, expireTime))
	return Reply.NewIntegerReply(1) // 设置成功，返回1
}

func execPExpireAt(db *redis.Database, args _type.Args) _interface.Reply {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(0) // key不存在，返回0
	}
	expireTime := time.Unix(0, ttl*int64(time.Millisecond)) // 以毫秒为单位的unix时间
	db.SetExpire(key, expireTime)
	db.ToAOF(utils.ToExpireCmd(key, expireTime))
	return Reply.NewIntegerReply(1) // 设置成功，返回1
}

func execTTL(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.NewIntegerReply(-1) // key存在但未设置过期时间，返回-1
	}
	ttl := expireTime.Sub(time.Now())
	return Reply.NewIntegerReply(int64(ttl / time.Second)) // 返回过期时间，以秒为单位
}

func execPTTL(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.NewIntegerReply(-1) // key存在但未设置过期时间，返回-1
	}
	ttl := expireTime.Sub(time.Now())
	return Reply.NewIntegerReply(int64(ttl / time.Millisecond)) // 返回过期时间，以毫秒为单位
}

func execExpireTime(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.NewIntegerReply(-1) // key存在但未设置过期时间，返回-1
	}
	return Reply.NewIntegerReply(expireTime.Unix()) // 返回过期时间，以秒为单位的unix时间
}
func execPExpireTime(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.NewIntegerReply(-1) // key存在但未设置过期时间，返回-1
	}
	return Reply.NewIntegerReply(expireTime.UnixMilli()) // 返回过期时间，以毫秒为单位的unix时间
}

func execPersist(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.NewIntegerReply(0) // key不存在(或已过期)，返回0
	}
	_, existed = db.GetExpireTime(key)
	if !existed {
		return Reply.NewIntegerReply(0) // key存在但未设置过期时间，返回0
	}
	db.Persist(key)
	db.ToAOF(utils.ToCmd("Persist", args...))
	return Reply.NewIntegerReply(1) // 取消过期成功，返回1
}

func execType(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity, existed := db.Get(key)
	if !existed {
		return Reply.NewStringReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return Reply.NewStringReply("string")
	case List.List[[]byte]:
		return Reply.NewStringReply("list")
	case Dict.Dict[string, []byte]:
		return Reply.NewStringReply("hash")
	case Set.Set[string]:
		return Reply.NewStringReply("set")
	case zset.ZSet[string]:
		return Reply.NewStringReply("zset")
	}
	return Reply.UnknownError()
}

func execRename(db *redis.Database, args _type.Args) _interface.Reply {
	key, newKey := string(args[0]), string(args[1])
	entity, existed := db.Get(key)
	if !existed {
		return Reply.StandardError("no such key") // 键不存在
	}
	// 重新设置newKey，旧值被覆盖
	db.Put(newKey, entity)
	// 设置ttl
	expireTime, ok := db.GetExpireTime(key)
	if ok {
		db.Persist(newKey) // 清除之前的ttl
		db.SetExpire(newKey, expireTime)
	}
	db.Remove(key) // 移除旧key
	db.ToAOF(utils.ToCmd("Rename", args...))
	return Reply.NewOkReply()
}

func execRenameNx(db *redis.Database, args _type.Args) _interface.Reply {
	key, newKey := string(args[0]), string(args[1])
	entity, existed := db.Get(key)
	if !existed {
		return Reply.StandardError("no such key") // 键不存在
	}
	_, existed = db.Get(newKey)
	if existed {
		return Reply.NewIntegerReply(0) // 新键已存在
	}
	// 重新设置newKey，旧值被覆盖
	db.Put(newKey, entity)
	// 设置ttl
	expireTime, ok := db.GetExpireTime(key)
	if ok {
		db.Persist(newKey) // 清除之前的ttl
		db.SetExpire(newKey, expireTime)
	}
	db.Remove(key) // 移除旧key
	db.ToAOF(utils.ToCmd("RenameNX", args...))
	return Reply.NewIntegerReply(1)
}

func execKeys(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.NewStringReply("This command is not supported temporarily")
}

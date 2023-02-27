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
	redis.RegisterCommand("Exists", execExists, utils.ReadAllKeys, -2, redis.ReadOnly)
	redis.RegisterCommand("Del", execDel, utils.WriteAllKeys, -2, redis.ReadWrite)
	redis.RegisterCommand("Expire", execExpire, utils.WriteFirstKey, 3, redis.ReadWrite)
	redis.RegisterCommand("ExpireAt", execExpireAt, utils.WriteFirstKey, 3, redis.ReadWrite)
	redis.RegisterCommand("TTL", execTTL, utils.ReadFirstKey, 2, redis.ReadOnly)
	redis.RegisterCommand("ExpireTime", execExpireTime, utils.ReadFirstKey, 2, redis.ReadOnly)
	redis.RegisterCommand("PExpire", execPExpire, utils.WriteFirstKey, 3, redis.ReadWrite)
	redis.RegisterCommand("PExpireAt", execPExpireAt, utils.WriteFirstKey, 3, redis.ReadWrite)
	redis.RegisterCommand("PTTL", execPTTL, utils.ReadFirstKey, 2, redis.ReadOnly)
	redis.RegisterCommand("PExpireTime", execPExpireTime, utils.ReadFirstKey, 2, redis.ReadOnly)
	redis.RegisterCommand("Persist", execPersist, utils.WriteFirstKey, 2, redis.ReadWrite)
	redis.RegisterCommand("Type", execType, utils.ReadFirstKey, 2, redis.ReadOnly)
	//RegisterCommand("Rename", execRename, prepareRename, undoRename, 3, flagReadOnly)
	//RegisterCommand("RenameNx", execRenameNx, prepareRename, undoRename, 3, flagReadOnly)
	//RegisterCommand("Keys", execKeys, noPrepare, nil, 2, flagReadOnly)
}

func execExists(db *redis.Database, args _type.Args) _interface.Reply {
	var count int64 = 0
	for _, key := range args {
		_, existed := db.Get(string(key))
		if existed {
			count++
		}
	}
	return Reply.MakeIntReply(count)
}

func execDel(db *redis.Database, args _type.Args) _interface.Reply {
	keys := make([]string, len(args))
	for i, key := range args {
		keys[i] = string(key)
	}
	count := db.Removes(keys...)
	if count > 0 {
		db.ToAof(utils.ToCmd("Del", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execExpire(db *redis.Database, args _type.Args) _interface.Reply {
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(0) // key不存在，返回0
	}
	ttl := time.Duration(ttlArg) * time.Second // 以秒为单位
	expireTime := time.Now().Add(ttl)
	db.SetExpire(key, expireTime)
	db.ToAof(utils.ToExpireCmd(key, expireTime))
	return Reply.MakeIntReply(1) // 设置成功，返回1
}

func execPExpire(db *redis.Database, args _type.Args) _interface.Reply {
	ttlArg, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(0) // key不存在，返回0
	}
	ttl := time.Duration(ttlArg) * time.Millisecond // 以毫秒为单位
	expireTime := time.Now().Add(ttl)
	db.SetExpire(key, expireTime)
	db.ToAof(utils.ToExpireCmd(key, expireTime))
	return Reply.MakeIntReply(1) // 设置成功，返回1
}

func execExpireAt(db *redis.Database, args _type.Args) _interface.Reply {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(0) // key不存在，返回0
	}
	expireTime := time.Unix(ttl, 0) // 以秒为单位的unix时间
	db.SetExpire(key, expireTime)
	db.ToAof(utils.ToExpireCmd(key, expireTime))
	return Reply.MakeIntReply(1) // 设置成功，返回1
}

func execPExpireAt(db *redis.Database, args _type.Args) _interface.Reply {
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("illegal integer for ttl")
	}
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(0) // key不存在，返回0
	}
	expireTime := time.Unix(0, ttl*int64(time.Millisecond)) // 以毫秒为单位的unix时间
	db.SetExpire(key, expireTime)
	db.ToAof(utils.ToExpireCmd(key, expireTime))
	return Reply.MakeIntReply(1) // 设置成功，返回1
}

func execTTL(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.MakeIntReply(-1) // key存在但未设置过期时间，返回-1
	}
	ttl := expireTime.Sub(time.Now())
	return Reply.MakeIntReply(int64(ttl / time.Second)) // 返回过期时间，以秒为单位
}

func execPTTL(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.MakeIntReply(-1) // key存在但未设置过期时间，返回-1
	}
	ttl := expireTime.Sub(time.Now())
	return Reply.MakeIntReply(int64(ttl / time.Millisecond)) // 返回过期时间，以毫秒为单位
}

func execExpireTime(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.MakeIntReply(-1) // key存在但未设置过期时间，返回-1
	}
	return Reply.MakeIntReply(expireTime.Unix()) // 返回过期时间，以秒为单位的unix时间
}
func execPExpireTime(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(-2) // key不存在(或已过期)，返回0
	}
	expireTime, existed := db.GetExpireTime(key)
	if !existed {
		return Reply.MakeIntReply(-1) // key存在但未设置过期时间，返回-1
	}
	return Reply.MakeIntReply(expireTime.UnixMilli()) // 返回过期时间，以毫秒为单位的unix时间
}

func execPersist(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	_, existed := db.Get(key)
	if !existed {
		return Reply.MakeIntReply(0) // key不存在(或已过期)，返回0
	}
	_, existed = db.GetExpireTime(key)
	if !existed {
		return Reply.MakeIntReply(0) // key存在但未设置过期时间，返回0
	}
	db.Persist(key)
	db.ToAof(utils.ToCmd("Persist", args...))
	return Reply.MakeIntReply(1) // 取消过期成功，返回1
}

func execType(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity, existed := db.Get(key)
	if !existed {
		return Reply.MakeStatusReply("none")
	}
	switch entity.Data.(type) {
	case []byte:
		return Reply.MakeStatusReply("string")
	case List.List[[]byte]:
		return Reply.MakeStatusReply("list")
	case Dict.Dict[string, []byte]:
		return Reply.MakeStatusReply("hash")
	case Set.Set[string]:
		return Reply.MakeStatusReply("set")
	case zset.ZSet[string]:
		return Reply.MakeStatusReply("zset")
	}
	return Reply.MakeUnknownErrReply()
}

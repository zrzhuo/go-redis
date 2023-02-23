package commands

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"strconv"
	"strings"
	"time"
)

// 注册命令
func init() {
	redis.RegisterCommand("Set", execSet, utils.WriteFirstKey, -3, redis.ReadWrite)
	redis.RegisterCommand("SetNX", execSetNX, utils.WriteFirstKey, 3, redis.ReadWrite)
	redis.RegisterCommand("SetEX", execSetEX, utils.WriteFirstKey, 4, redis.ReadWrite)
	redis.RegisterCommand("Get", execGet, utils.ReadFirstKey, 2, redis.ReadOnly)
}

func execSet(db *redis.Database, args _type.Args) _interface.Reply {
	policy := ""
	ttl := int64(0)
	for i := 2; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		// 解析XX和NX：参数中只能存在一个XX或NX
		case "XX":
			if policy == "NX" {
				return Reply.MakeSyntaxErrReply()
			}
			policy = "XX"
		case "NX":
			if policy == "XX" {
				return Reply.MakeSyntaxErrReply()
			}
			policy = "NX"
		// 解析EX和PX：参数中只能存在一个EX或PX，其EX或PX之后必须紧跟时间参数
		case "EX":
			if ttl != 0 || i+1 > len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttlArg <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			ttl = ttlArg * 1000 // 以秒为单位
			i++                 // 时间参数无需再解析
		case "PX":
			if ttl != 0 || i+1 > len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttlArg <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			ttl = ttlArg // 以毫秒为单位
			i++          // 时间参数无需再解析
		default:
			return Reply.MakeSyntaxErrReply()
		}
	}

	key := string(args[0])
	entity := _type.NewEntity(args[1])
	var res int
	switch policy {
	case "XX":
		res = db.PutIfExists(key, entity)
	case "NX":
		res = db.PutIfAbsent(key, entity)
	default:
		res = db.Put(key, entity)
	}

	if res > 0 {
		if ttl > 0 {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.SetExpire(key, expireTime)
			db.ToAof(utils.ToCmdLine3("Set", args[0], args[1]))
			//db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
		} else {
			db.CancelExpire(key)
			db.ToAof(utils.ToCmdLine3("set", args...))
		}
		return Reply.MakeOkReply()
	}
	return Reply.MakeNullBulkReply()
}

func execSetNX(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity := _type.NewEntity(args[1])
	result := db.PutIfAbsent(key, entity)
	db.ToAof(utils.ToCmdLine3("SetNX", args...))
	return Reply.MakeIntReply(int64(result))
}

func execSetEX(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	ttl, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.MakeErrReply("invalid expire time")
	}
	if ttl <= 0 {
		return Reply.MakeErrReply("invalid expire time")
	}
	entity := _type.NewEntity(args[2])
	db.Put(key, entity)
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	db.SetExpire(key, expireTime)
	db.ToAof(utils.ToCmdLine3("SetEX", args...))
	//db.addAof(aof.MakeExpireCmd(key, expireTime).Args)
	return Reply.MakeOkReply()
}

func execGet(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	bytes, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	return Reply.MakeBulkReply(bytes)
}
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
	redis.RegisterCommand("Set", execSet, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("SetNX", execSetNX, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("SetEX", execSetEX, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("Get", execGet, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("GetEX", execGetEX, utils.WriteFirst, -2, redis.ReadWrite)
	redis.RegisterCommand("GetSet", execGetSet, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("GetDel", execGetDel, utils.WriteFirst, 2, redis.ReadWrite)
	redis.RegisterCommand("StrLen", execStrLen, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("Append", execAppend, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("MSet", execMSet, utils.WriteEven, -3, redis.ReadWrite)
	redis.RegisterCommand("MSetNX", execMSetNX, utils.WriteEven, -3, redis.ReadWrite)
	redis.RegisterCommand("MGet", execMGet, utils.ReadAll, -2, redis.ReadOnly)
	redis.RegisterCommand("Incr", execIncr, utils.WriteFirst, 2, redis.ReadWrite)
	redis.RegisterCommand("IncrBy", execIncrBy, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("IncrByFloat", execIncrByFloat, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("Decr", execDecr, utils.WriteFirst, 2, redis.ReadWrite)
	redis.RegisterCommand("DecrBy", execDecrBy, utils.WriteFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("SetRange", execSetRange, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("GetRange", execGetRange, utils.ReadFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("SetBit", execSetBit, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("GetBit", execGetBit, utils.ReadFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("BitCount", execBitCount, utils.ReadFirst, -2, redis.ReadWrite)
	redis.RegisterCommand("BitPos", execBitPos, utils.ReadFirst, -3, redis.ReadWrite)
}

func execSet(db *redis.Database, args _type.Args) _interface.Reply {
	policy := ""
	ttl := int64(0)
	// 参数解析
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
			if ttl != 0 || i+1 >= len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			ttlArg, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttlArg <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			ttl = ttlArg * 1000 // 以秒为单位
			i++                 // 时间参数无需再解析
		case "PX":
			if ttl != 0 || i+1 >= len(args) {
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
	// put
	key := string(args[0])
	entity := _type.NewEntity(args[1])
	var res int
	switch policy {
	case "XX":
		res = db.PutIfExists(key, entity)
	case "NX":
		res = db.PutIfAbsent(key, entity)
	default:
		db.Put(key, entity)
		res = 1
	}
	// aof和expire
	if res > 0 {
		db.ToAOF(utils.ToCmd("Set", args[0], args[1]))
		if ttl > 0 {
			expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
			db.SetExpire(key, expireTime)
			db.ToAOF(utils.ToExpireCmd(key, expireTime))
		} else {
			db.Persist(key)
		}
		return Reply.MakeOkReply()
	}
	return Reply.MakeNullBulkReply()
}

func execSetNX(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity := _type.NewEntity(args[1])
	result := db.PutIfAbsent(key, entity)
	db.ToAOF(utils.ToCmd("SetNX", args...))
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
	// put
	db.Put(key, entity)
	db.ToAOF(utils.ToCmd("SetEX", args...))
	expireTime := time.Now().Add(time.Duration(ttl) * time.Millisecond)
	// expire
	db.SetExpire(key, expireTime)
	db.ToAOF(utils.ToExpireCmd(key, expireTime))
	return Reply.MakeOkReply()
}

func execGet(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	val, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	if val == nil {
		return Reply.MakeNullBulkReply()
	}
	return Reply.MakeBulkReply(val)
}

func execGetEX(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	val, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	if val == nil {
		return Reply.MakeNullBulkReply()
	}
	// 解析过期策略和过期时间
	flag := false // 只能存在一个EX、PX、EXAT、PXAT、PERSIST
	var expireTime time.Time
	for i := 1; i < len(args); i++ {
		arg := strings.ToUpper(string(args[i]))
		switch arg {
		case "EX":
			if flag || i+1 >= len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			flag = true
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttl <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			expireTime = time.Now().Add(time.Duration(ttl) * time.Second) // 以秒为单位
			i++
		case "PX":
			if flag || i+1 >= len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			flag = true
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttl <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			expireTime = time.Now().Add(time.Duration(ttl) * time.Millisecond) // 以毫秒为单位
			i++
		case "EXAT":
			if flag || i+1 >= len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			flag = true
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttl <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			expireTime = time.Unix(ttl, 0) // 以秒为单位的unix时间
			i++
		case "PXAT":
			if flag || i+1 >= len(args) {
				return Reply.MakeSyntaxErrReply()
			}
			flag = true
			ttl, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil || ttl <= 0 {
				return Reply.MakeErrReply("invalid expire time")
			}
			expireTime = time.Unix(0, ttl*int64(time.Millisecond)) // 以毫秒为单位的unix时间
			i++
		case "PERSIST":
			if flag {
				return Reply.MakeSyntaxErrReply()
			}
			db.Persist(key)                           // persist
			db.ToAOF(utils.ToCmd("Persist", args[0])) // aof
		default:
			return Reply.MakeSyntaxErrReply()
		}
	}
	// expire
	if flag {
		db.SetExpire(key, expireTime)
		db.ToAOF(utils.ToExpireCmd(key, expireTime))
	}
	return Reply.MakeBulkReply(val)
}

func execGetSet(db *redis.Database, args _type.Args) _interface.Reply {
	key, newVal := string(args[0]), args[1]
	oldVal, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	entity := _type.NewEntity(newVal)
	db.Put(key, entity)
	db.Persist(key)                       // persist
	db.ToAOF(utils.ToCmd("Set", args...)) // aof
	if oldVal == nil {
		return Reply.MakeNullBulkReply() // 旧值不存在
	}
	return Reply.MakeBulkReply(oldVal) // 返回旧值
}

func execGetDel(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	val, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	if val == nil {
		return Reply.MakeNullBulkReply()
	}
	db.Remove(key)
	db.ToAOF(utils.ToCmd("Del", args...))
	return Reply.MakeBulkReply(val)
}

func execStrLen(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	val, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	if val == nil {
		return Reply.MakeIntReply(0)
	}
	return Reply.MakeIntReply(int64(len(val)))
}

func execAppend(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	val, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	val = append(val, args[1]...)
	entity := _type.NewEntity(val)
	db.Put(key, entity)
	db.ToAOF(utils.ToCmd("append", args...))
	return Reply.MakeIntReply(int64(len(val)))
}

func execMSet(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args)%2 != 0 {
		return Reply.MakeSyntaxErrReply()
	}
	for i := 0; i < len(args)/2; i++ {
		key, val := string(args[2*i]), args[2*i+1]
		entity := _type.NewEntity(val)
		db.Put(key, entity)
	}
	db.ToAOF(utils.ToCmd("MSet", args...))
	return Reply.MakeOkReply()
}

func execMSetNX(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args)%2 != 0 {
		return Reply.MakeSyntaxErrReply()
	}
	// 判断是否所有key都不存在
	for i := 0; i < len(args)/2; i++ {
		key := string(args[2*i])
		_, existed := db.Get(key)
		if existed {
			return Reply.MakeIntReply(0)
		}
	}
	// put所有key
	for i := 0; i < len(args)/2; i++ {
		key, val := string(args[2*i]), args[2*i+1]
		entity := _type.NewEntity(val)
		db.Put(key, entity)
	}
	db.ToAOF(utils.ToCmd("MSetNX", args...))
	return Reply.MakeIntReply(1)
}

func execMGet(db *redis.Database, args _type.Args) _interface.Reply {
	result := make([][]byte, len(args))
	for i := 0; i < len(args); i++ {
		key := string(args[i])
		val, errReply := db.GetString(key)
		if errReply != nil {
			result[i] = nil
			continue
		}
		if val == nil {
			result[i] = nil
			continue
		}
		result[i] = val
	}
	return Reply.MakeArrayReply(result)
}

func execBitCount(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execBitPos(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execIncr(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execIncrByFloat(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execSetBit(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execDecr(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execDecrBy(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execGetBit(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execGetRange(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execIncrBy(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execSetRange(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

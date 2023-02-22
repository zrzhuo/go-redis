package commands

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	reply2 "go-redis/resp/reply"
)

// 注册命令
func init() {
	redis.RegisterCommand("Set", execSet, utils.WriteFirstKey, -3, redis.ReadWrite)
	//redis.RegisterCommand("SetNX", execSet, 3, redis.ReadWrite)
	//redis.RegisterCommand("SetEX", execSet, 4, redis.ReadWrite)
	redis.RegisterCommand("Get", execGet, utils.ReadFirstKey, 2, redis.ReadOnly)
	//redis.RegisterCommand("GetNX", execSet, -3, redis.ReadWrite)
}

func execSet(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity := _type.NewEntity(args[1])
	result := db.PutEntity(key, entity)
	if result > 0 {
		return &reply2.OkReply{}
	}
	return &reply2.NullBulkReply{}
}

func execGet(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	bytes, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	return reply2.MakeBulkReply(bytes)
}

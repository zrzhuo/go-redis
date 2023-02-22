package commands

import (
	"go-redis/database"
	"go-redis/database/commands/common"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/redis/resp/reply"
)

// 注册命令
func init() {
	database.RegisterCommand("Set", execSet, common.WriteFirstKey, -3, database.ReadWrite)
	//database.RegisterCommand("SetNX", execSet, 3, database.ReadWrite)
	//database.RegisterCommand("SetEX", execSet, 4, database.ReadWrite)
	database.RegisterCommand("Get", execGet, common.ReadFirstKey, 2, database.ReadOnly)
	//database.RegisterCommand("GetNX", execSet, -3, database.ReadWrite)
}

func execSet(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity := _type.NewEntity(args[1])
	result := db.PutEntity(key, entity)
	if result > 0 {
		return &Reply.OkReply{}
	}
	return &Reply.NullBulkReply{}
}

func execGet(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	bytes, errReply := db.GetString(key)
	if errReply != nil {
		return errReply
	}
	return Reply.MakeBulkReply(bytes)
}

package commands

import (
	"go-redis/database"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
)

// 注册命令
func init() {
	database.RegisterCommand("Set", setExec, writeFirstKey, -3, database.ReadWrite)
	//database.RegisterCommand("SetNX", setExec, 3, database.ReadWrite)
	//database.RegisterCommand("SetEX", setExec, 4, database.ReadWrite)
	database.RegisterCommand("Get", getExec, readFirstKey, 2, database.ReadOnly)
	//database.RegisterCommand("GetNX", setExec, -3, database.ReadWrite)
}

func getAsString(db *database.Database, key string) ([]byte, _interface.Reply) {
	entity, ok := db.GetEntity(key)
	if !ok {
		return nil, &reply.NullBulkReply{}
	}
	bytes, ok := entity.Data.([]byte)
	if !ok {
		return nil, &reply.WrongTypeErrReply{}
	}
	return bytes, nil
}

func setExec(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	entity := &_type.Entity{
		Data: args[1],
	}
	result := db.PutEntity(key, entity)
	if result > 0 {
		return &reply.OkReply{}
	}
	return &reply.NullBulkReply{}
}

func getExec(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	bytes, errReply := getAsString(db, key)
	if errReply != nil {
		return errReply
	}
	return reply.MakeBulkReply(bytes)
}

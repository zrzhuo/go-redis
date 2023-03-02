package commands

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
)

func init() {
	redis.RegisterCommand("SetBit", execSetBit, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("GetBit", execGetBit, utils.ReadFirst, 3, redis.ReadWrite)
	redis.RegisterCommand("BitCount", execBitCount, utils.ReadFirst, -2, redis.ReadWrite)
	redis.RegisterCommand("BitPos", execBitPos, utils.ReadFirst, -3, redis.ReadWrite)
}

func execSetBit(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

func execGetBit(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

func execBitCount(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

func execBitPos(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

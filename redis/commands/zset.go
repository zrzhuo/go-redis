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
	redis.RegisterCommand("ZAdd", execZAdd, utils.WriteFirst, -4, redis.ReadWrite)
	redis.RegisterCommand("ZRem", execZRem, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("ZRemRangeByScore", execZRemRangeByScore, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("ZRemRangeByRank", execZRemRangeByRank, utils.WriteFirst, 4, redis.ReadWrite)
	redis.RegisterCommand("ZCard", execZCard, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("ZScore", execZScore, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("ZCount", execZCount, utils.ReadFirst, 4, redis.ReadOnly)
	redis.RegisterCommand("ZRank", execZRank, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("ZRevRank", execZRevRank, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("ZRange", execZRange, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZRevRange", execZRevRange, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZRangeByScore", execZRangeByScore, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZRevRangeByScore", execZRevRangeByScore, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZPopMin", execZPopMin, utils.WriteFirst, -2, redis.ReadWrite)
	redis.RegisterCommand("ZIncrBy", execZIncrBy, utils.WriteFirst, 4, redis.ReadWrite)
}

func execZAdd(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args)%2 != 1 {
		return Reply.MakeArgNumErrReply("ZAdd")
	}
	key, num := string(args[0]), (len(args)-1)/2
	zset, _, errReply := db.GetOrInitZSet(key)
	if errReply != nil {
		return errReply
	}
	count := 0
	for i := 0; i < num; i++ {
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(args[2*i+1]), 64)
		if err != nil {
			return Reply.MakeErrReply("value is not a valid float")
		}
		count += zset.Add(member, score)
	}
	db.ToAOF(utils.ToCmd("ZAdd", args...))
	return Reply.MakeIntReply(int64(count))
}

func execZRank(db *redis.Database, args _type.Args) _interface.Reply {
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeNullBulkReply()
	}
	rank := zset.GetRank(member, false)
	if rank < 0 {
		return Reply.MakeNullBulkReply()
	}
	return Reply.MakeIntReply(int64(rank))
}

func execZScore(db *redis.Database, args _type.Args) _interface.Reply {
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeNullBulkReply()
	}
	score, existed := zset.Get(member)
	if !existed {
		return Reply.MakeNullBulkReply()
	}
	value := strconv.FormatFloat(score, 'f', -1, 64)
	return Reply.MakeBulkReply([]byte(value))
}

func execZIncrBy(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

func execZCard(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRange(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZCount(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRevRange(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRevRank(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZPopMin(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRangeByScore(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRem(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRemRangeByRank(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRemRangeByScore(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRevRangeByScore(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

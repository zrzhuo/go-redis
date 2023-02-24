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
	redis.RegisterCommand("ZAdd", execZAdd, utils.WriteFirstKey, -4, redis.ReadWrite)
	redis.RegisterCommand("ZScore", execZScore, utils.ReadFirstKey, 3, redis.ReadOnly)
	//RegisterCommand("ZIncrBy", execZIncrBy, writeFirstKey, undoZIncr, 4, flagWrite)
	redis.RegisterCommand("ZRank", execZRank, utils.ReadFirstKey, 3, redis.ReadOnly)
	//RegisterCommand("ZCount", execZCount, readFirstKey, nil, 4, flagReadOnly)
	//RegisterCommand("ZRevRank", execZRevRank, readFirstKey, nil, 3, flagReadOnly)
	//RegisterCommand("ZCard", execZCard, readFirstKey, nil, 2, flagReadOnly)
	//RegisterCommand("ZRange", execZRange, readFirstKey, nil, -4, flagReadOnly)
	//RegisterCommand("ZRangeByScore", execZRangeByScore, readFirstKey, nil, -4, flagReadOnly)
	//RegisterCommand("ZRevRange", execZRevRange, readFirstKey, nil, -4, flagReadOnly)
	//RegisterCommand("ZRevRangeByScore", execZRevRangeByScore, readFirstKey, nil, -4, flagReadOnly)
	//RegisterCommand("ZPopMin", execZPopMin, writeFirstKey, rollbackFirstKey, -2, flagWrite)
	//RegisterCommand("ZRem", execZRem, writeFirstKey, undoZRem, -3, flagWrite)
	//RegisterCommand("ZRemRangeByScore", execZRemRangeByScore, writeFirstKey, rollbackFirstKey, 4, flagWrite)
	//RegisterCommand("ZRemRangeByRank", execZRemRangeByRank, writeFirstKey, rollbackFirstKey, 4, flagWrite)
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
	//db.addAof(utils.ToCmdLine("zadd", args...))
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

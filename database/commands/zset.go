package commands

import (
	"go-redis/database"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
	"strconv"
)

func init() {
	database.RegisterCommand("ZAdd", execZAdd, writeFirstKey, -4, database.ReadWrite)
	database.RegisterCommand("ZScore", execZScore, readFirstKey, 3, database.ReadOnly)
	//RegisterCommand("ZIncrBy", execZIncrBy, writeFirstKey, undoZIncr, 4, flagWrite)
	database.RegisterCommand("ZRank", execZRank, readFirstKey, 3, database.ReadOnly)
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

func execZAdd(db *database.Database, args _type.Args) _interface.Reply {
	if len(args)%2 != 1 {
		return reply.MakeArgNumErrReply("number of parameters must be odd.")
	}
	key := string(args[0])
	num := (len(args) - 1) / 2
	zset, _, errReply := db.GetOrInitZSet(key)
	if errReply != nil {
		return errReply
	}
	count := 0
	for i := 0; i < num; i++ {
		member := string(args[2*i+2])
		score, err := strconv.ParseFloat(string(args[2*i+1]), 64)
		if err != nil {
			return reply.MakeErrReply("value is not a valid float")
		}
		if zset.Add(member, score) {
			count++
		}
	}
	//db.addAof(utils.ToCmdLine3("zadd", args...))
	return reply.MakeIntReply(int64(count))
}

func execZRank(db *database.Database, args _type.Args) _interface.Reply {
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return reply.MakeNullBulkReply()
	}
	rank := zset.GetRank(member, false)
	if rank < 0 {
		return reply.MakeNullBulkReply()
	}
	return reply.MakeIntReply(int64(rank))
}

func execZScore(db *database.Database, args _type.Args) _interface.Reply {
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return reply.MakeNullBulkReply()
	}
	score, existed := zset.Get(member)
	if !existed {
		return reply.MakeNullBulkReply()
	}
	value := strconv.FormatFloat(score, 'f', -1, 64)
	return reply.MakeBulkReply([]byte(value))
}

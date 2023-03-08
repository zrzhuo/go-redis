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
	redis.RegisterCommand("ZCount", execZCount, utils.ReadFirst, 4, redis.ReadOnly)
	redis.RegisterCommand("ZScore", execZScore, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("ZRank", execZRank, utils.ReadFirst, -3, redis.ReadOnly)
	redis.RegisterCommand("ZRevRank", execZRevRank, utils.ReadFirst, -3, redis.ReadOnly)
	redis.RegisterCommand("ZRange", execZRange, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZRevRange", execZRevRange, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZRangeByScore", execZRangeByScore, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZRevRangeByScore", execZRevRangeByScore, utils.ReadFirst, -4, redis.ReadOnly)
	redis.RegisterCommand("ZPopMin", execZPopMin, utils.WriteFirst, -2, redis.ReadWrite)
	redis.RegisterCommand("ZIncrBy", execZIncrBy, utils.WriteFirst, 4, redis.ReadWrite)
}

func execZAdd(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args)%2 != 1 {
		return Reply.ArgNumError("ZAdd")
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
			return Reply.StandardError("value is not a valid float")
		}
		count += zset.Add(member, score)
	}
	if count > 0 {
		db.ToAOF(utils.ToCmd("ZAdd", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execZRem(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeIntReply(0)
	}
	count := 0
	for i := 1; i < len(args); i++ {
		member := string(args[i])
		count += zset.Remove(member)
	}
	if count > 0 {
		db.ToAOF(utils.ToCmd("ZRem", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execZRemRangeByRank(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeIntReply(0)
	}
	left, right := utils.ParseRange(int(start), int(stop), zset.Len())
	if left < 0 {
		return Reply.MakeNilBulkReply()
	}
	count := zset.RemoveRangeByRank(left, right+1)
	if count > 0 {
		db.ToAOF(utils.ToCmd("ZRemRangeByRank", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execZRemRangeByScore(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	min, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return Reply.StandardError("min or max is not a float")
	}
	max, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return Reply.StandardError("min or max is not a float")
	}
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeIntReply(0)
	}
	count := zset.RemoveRangeByScore(min, max)
	if count > 0 {
		db.ToAOF(utils.ToCmd("ZRemRangeByScore", args...))
	}
	return Reply.MakeIntReply(int64(count))
}

func execZCard(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeIntReply(0)
	}
	return Reply.MakeIntReply(int64(zset.Len()))
}

func execZCount(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	min, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil {
		return Reply.StandardError("min or max is not a float")
	}
	max, err := strconv.ParseFloat(string(args[2]), 64)
	if err != nil {
		return Reply.StandardError("min or max is not a float")
	}
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeIntReply(0)
	}
	count := zset.RangeLen(min, max)
	return Reply.MakeIntReply(int64(count))
}

func execZScore(db *redis.Database, args _type.Args) _interface.Reply {
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeNilBulkReply()
	}
	score, existed := zset.GetScore(member)
	if !existed {
		return Reply.MakeNilBulkReply()
	}
	value := strconv.FormatFloat(score, 'f', -1, 64)
	return Reply.MakeBulkReply([]byte(value))
}

func execZRank(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args) > 3 {
		return Reply.ArgNumError("ZRank")
	}
	withScore := false
	if len(args) == 3 {
		if string(args[2]) != "withscore" {
			return Reply.SyntaxError()
		}
		withScore = true
	}
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeNilBulkReply()
	}
	rank, ok := zset.GetRank(member, false)
	if withScore {
		if !ok {
			return Reply.MakeEmptyArrayReply()
		}
		score, _ := zset.GetScore(member)
		rankStr := strconv.FormatInt(int64(rank), 10)
		scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
		return Reply.StringToArrayReply(rankStr, scoreStr)
	} else {
		if !ok {
			return Reply.MakeNilBulkReply()
		}
		return Reply.MakeIntReply(int64(rank))
	}
}

func execZRevRank(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args) > 3 {
		return Reply.ArgNumError("ZRank")
	}
	withScore := false
	if len(args) == 3 {
		if string(args[2]) != "withscore" {
			return Reply.SyntaxError()
		}
		withScore = true
	}
	key, member := string(args[0]), string(args[1])
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeNilBulkReply()
	}
	rank, ok := zset.GetRank(member, true)
	if withScore {
		if !ok {
			return Reply.MakeEmptyArrayReply()
		}
		score, _ := zset.GetScore(member)
		rankStr := strconv.FormatInt(int64(rank), 10)
		scoreStr := strconv.FormatFloat(score, 'f', -1, 64)
		return Reply.StringToArrayReply(rankStr, scoreStr)
	} else {
		if !ok {
			return Reply.MakeNilBulkReply()
		}
		return Reply.MakeIntReply(int64(rank))
	}
}

func execZRange(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args) > 4 {
		return Reply.ArgNumError("ZRange")
	}
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "withscores" {
			return Reply.SyntaxError()
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeEmptyArrayReply()
	}
	left, right := utils.ParseRange(int(start), int(stop), zset.Len())
	if left < 0 {
		return Reply.MakeNilBulkReply()
	}
	if withScores {
		result := make([]string, 2*(right-left))
		i := 0
		consumer := func(member string, score float64) bool {
			result[i] = member
			i++
			result[i] = strconv.FormatFloat(score, 'f', -1, 64)
			i++
			return true
		}
		zset.ForEach(left, right, false, consumer)
		return Reply.StringToArrayReply(result...)
	} else {
		result := make([]string, right-left)
		i := 0
		consumer := func(member string, score float64) bool {
			result[i] = member
			i++
			return true
		}
		zset.ForEach(left, right, false, consumer)
		return Reply.StringToArrayReply(result...)
	}
}

func execZRevRange(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args) > 4 {
		return Reply.ArgNumError("ZRevRange")
	}
	withScores := false
	if len(args) == 4 {
		if string(args[3]) != "withscores" {
			return Reply.SyntaxError()
		}
		withScores = true
	}
	key := string(args[0])
	start, err := strconv.ParseInt(string(args[1]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	stop, err := strconv.ParseInt(string(args[2]), 10, 64)
	if err != nil {
		return Reply.StandardError("value is not an integer or out of range")
	}
	zset, errReply := db.GetZSet(key)
	if errReply != nil {
		return errReply
	}
	if zset == nil {
		return Reply.MakeEmptyArrayReply()
	}
	left, right := utils.ParseRange(int(start), int(stop), zset.Len())
	if left < 0 {
		return Reply.MakeNilBulkReply()
	}
	if withScores {
		result := make([]string, 2*(right-left))
		i := 0
		consumer := func(member string, score float64) bool {
			result[i] = member
			i++
			result[i] = strconv.FormatFloat(score, 'f', -1, 64)
			i++
			return true
		}
		zset.ForEach(left, right, true, consumer)
		return Reply.StringToArrayReply(result...)
	} else {
		result := make([]string, right-left)
		i := 0
		consumer := func(member string, score float64) bool {
			result[i] = member
			i++
			return true
		}
		zset.ForEach(left, right, true, consumer)
		return Reply.StringToArrayReply(result...)
	}
}

func execZRangeByScore(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZRevRangeByScore(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZPopMin(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}
func execZIncrBy(db *redis.Database, args _type.Args) _interface.Reply {
	return Reply.MakeStatusReply("This command is not supported temporarily")
}

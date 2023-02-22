package commands

import (
	"go-redis/database"
	"go-redis/database/commands/common"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/resp/reply"
)

func init() {
	database.RegisterCommand("SAdd", execSAdd, common.WriteFirstKey, -3, database.ReadWrite)
	//RegisterCommand("SIsMember", execSIsMember, readFirstKey, nil, 3, flagReadOnly)
	//RegisterCommand("SRem", execSRem, writeFirstKey, undoSetChange, -3, flagWrite)
	//RegisterCommand("SPop", execSPop, writeFirstKey, undoSetChange, -2, flagWrite)
	//RegisterCommand("SCard", execSCard, readFirstKey, nil, 2, flagReadOnly)
	database.RegisterCommand("SMembers", execSMembers, common.ReadFirstKey, 2, database.ReadWrite)
	//RegisterCommand("SInter", execSInter, prepareSetCalculate, nil, -2, flagReadOnly)
	//RegisterCommand("SInterStore", execSInterStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite)
	//RegisterCommand("SUnion", execSUnion, prepareSetCalculate, nil, -2, flagReadOnly)
	//RegisterCommand("SUnionStore", execSUnionStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite)
	//RegisterCommand("SDiff", execSDiff, prepareSetCalculate, nil, -2, flagReadOnly)
	//RegisterCommand("SDiffStore", execSDiffStore, prepareSetCalculateStore, rollbackFirstKey, -3, flagWrite)
	//RegisterCommand("SRandMember", execSRandMember, readFirstKey, nil, -2, flagReadOnly)
}

func execSAdd(db *database.Database, args _type.Args) _interface.Reply {
	key, num := string(args[0]), len(args)-1
	set, _, errReply := db.GetOrInitSet(key)
	if errReply != nil {
		return errReply
	}
	count := 0
	for i := 0; i < num; i++ {
		member := string(args[i+1])
		count += set.Add(member)
	}
	//db.addAof(utils.ToCmdLine3("sadd", args...))
	return reply.MakeIntReply(int64(count))
}

func execSMembers(db *database.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeEmptyMultiBulkReply()
	}
	members, size := set.Members(), set.Len()
	result := make([][]byte, size)
	for i := 0; i < size; i++ {
		result[i] = []byte(members[i])
	}
	return reply.MakeMultiBulkReply(result)
}

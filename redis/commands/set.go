package commands

import (
	Set "go-redis/datastruct/set"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis"
	"go-redis/redis/utils"
	"go-redis/resp/reply"
	"strconv"
)

func init() {
	redis.RegisterCommand("SAdd", execSAdd, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("SRem", execSRem, utils.WriteFirst, -3, redis.ReadWrite)
	redis.RegisterCommand("SPop", execSPop, utils.WriteFirst, -2, redis.ReadWrite)
	redis.RegisterCommand("SRandMember", execSRandMember, utils.ReadFirst, -2, redis.ReadOnly)
	redis.RegisterCommand("SCard", execSCard, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("SIsMember", execSIsMember, utils.ReadFirst, 3, redis.ReadOnly)
	redis.RegisterCommand("SMembers", execSMembers, utils.ReadFirst, 2, redis.ReadOnly)
	redis.RegisterCommand("SInter", execSInter, utils.ReadAll, -2, redis.ReadOnly)
	redis.RegisterCommand("SUnion", execSUnion, utils.ReadAll, -2, redis.ReadOnly)
	redis.RegisterCommand("SDiff", execSDiff, utils.ReadAll, -2, redis.ReadOnly)
	redis.RegisterCommand("SInterStore", execSInterStore, utils.WriteFirstReadOthers, -3, redis.ReadWrite)
	redis.RegisterCommand("SUnionStore", execSUnionStore, utils.WriteFirstReadOthers, -3, redis.ReadWrite)
	redis.RegisterCommand("SDiffStore", execSDiffStore, utils.WriteFirstReadOthers, -3, redis.ReadWrite)
}

func execSAdd(db *redis.Database, args _type.Args) _interface.Reply {
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
	db.ToAOF(utils.ToCmd("SAdd", args...))
	return reply.MakeIntReply(int64(count))
}

func execSRem(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}
	count := 0
	for i := 1; i < len(args); i++ {
		member := string(args[i])
		count += set.Remove(member)
	}
	if set.Len() == 0 {
		db.Remove(key)
	}
	if count > 0 {
		db.ToAOF(utils.ToCmd("SRem", args...))
	}
	return reply.MakeIntReply(int64(count))
}

func execSPop(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args) > 2 {
		return reply.MakeSyntaxErrReply()
	}
	key := string(args[0])
	hasCount := len(args) == 2
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		if !hasCount {
			return reply.MakeNullBulkReply()
		} else {
			return reply.MakeEmptyArrayReply()
		}
	}
	if !hasCount {
		member := set.RandomDistinctMembers(1)[0]
		set.Remove(member)
		db.ToAOF(utils.ToCmd("SRem", args[0], []byte(member)))
		return reply.MakeBulkReply([]byte(member))
	} else {
		count, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return reply.MakeErrReply("value is not an integer or out of range")
		}
		members := set.RandomDistinctMembers(int(count))
		for _, member := range members {
			set.Remove(member)
			db.ToAOF(utils.ToCmd("SRem", args[0], []byte(member)))
		}
		return reply.StringToArrayReply(members...)
	}
}

func execSRandMember(db *redis.Database, args _type.Args) _interface.Reply {
	if len(args) > 2 {
		return reply.MakeSyntaxErrReply()
	}
	key := string(args[0])
	hasCount := len(args) == 2
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		if !hasCount {
			return reply.MakeNullBulkReply()
		} else {
			return reply.MakeEmptyArrayReply()
		}
	}
	if !hasCount {
		member := set.RandomDistinctMembers(1)[0]
		return reply.MakeBulkReply([]byte(member))
	} else {
		count, err := strconv.ParseInt(string(args[1]), 10, 64)
		if err != nil {
			return reply.MakeErrReply("value is not an integer or out of range")
		}
		members := set.RandomDistinctMembers(int(count))
		return reply.StringToArrayReply(members...)
	}
}

func execSCard(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}
	return reply.MakeIntReply(int64(set.Len()))
}

func execSIsMember(db *redis.Database, args _type.Args) _interface.Reply {
	key, member := string(args[0]), string(args[0])
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeIntReply(0)
	}
	if set.Contain(member) {
		return reply.MakeIntReply(1)
	}
	return reply.MakeIntReply(0)
}

func execSMembers(db *redis.Database, args _type.Args) _interface.Reply {
	key := string(args[0])
	set, errReply := db.GetSet(key)
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeEmptyArrayReply()
	}
	members, size := set.Members(), set.Len()
	result := make([][]byte, size)
	for i := 0; i < size; i++ {
		result[i] = []byte(members[i])
	}
	return reply.MakeArrayReply(result)
}

func execSInter(db *redis.Database, args _type.Args) _interface.Reply {
	set := Set.MakeSimpleSet[string]()
	for _, arg := range args {
		anoSet, errReply := db.GetSet(string(arg))
		if errReply != nil {
			return errReply
		}
		if anoSet == nil {
			return reply.MakeEmptyArrayReply()
		}
		set = set.Inter(anoSet)
		if set.Len() == 0 {
			return reply.MakeEmptyArrayReply()
		}
	}
	return reply.StringToArrayReply(set.Members()...)
}

func execSUnion(db *redis.Database, args _type.Args) _interface.Reply {
	set := Set.MakeSimpleSet[string]()
	for _, arg := range args {
		anoSet, errReply := db.GetSet(string(arg))
		if errReply != nil {
			return errReply
		}
		if anoSet == nil {
			continue
		}
		set = set.Union(anoSet)
	}
	if set.Len() == 0 {
		return reply.MakeEmptyArrayReply()
	}
	return reply.StringToArrayReply(set.Members()...)
}

func execSDiff(db *redis.Database, args _type.Args) _interface.Reply {
	set, errReply := db.GetSet(string(args[0]))
	if errReply != nil {
		return errReply
	}
	if set == nil {
		return reply.MakeEmptyArrayReply()
	}
	// 求其余set的并集，然后在求差集，可提高效率
	unionSet := Set.MakeSimpleSet[string]()
	for _, arg := range args[1:] {
		anoSet, errReply := db.GetSet(string(arg))
		if errReply != nil {
			return errReply
		}
		if anoSet == nil {
			continue
		}
		unionSet = unionSet.Union(anoSet)
	}
	result := set.Diff(unionSet)
	if result.Len() == 0 {
		return reply.MakeEmptyArrayReply()
	}
	return reply.StringToArrayReply(result.Members()...)
}

func execSInterStore(db *redis.Database, args _type.Args) _interface.Reply {
	dest := string(args[0])
	set := Set.MakeSimpleSet[string]()
	for _, arg := range args[1:] {
		anoSet, errReply := db.GetSet(string(arg))
		if errReply != nil {
			return errReply
		}
		if anoSet == nil {
			db.Remove(dest) // 清掉dest
			db.ToAOF(utils.StringToCmd("Del", dest))
			return reply.MakeIntReply(0)
		}
		set = set.Inter(anoSet)
		if set.Len() == 0 {
			db.Remove(dest) // 清掉dest
			db.ToAOF(utils.StringToCmd("Del", dest))
			return reply.MakeIntReply(0)
		}
	}
	db.Remove(dest) // 清掉dest
	db.ToAOF(utils.StringToCmd("Del", dest))
	db.Put(dest, _type.NewEntity(set))
	db.ToAOF(utils.StringToCmd("SAdd", set.Members()...))
	return reply.MakeIntReply(int64(set.Len()))
}

func execSUnionStore(db *redis.Database, args _type.Args) _interface.Reply {
	dest := string(args[0])
	set := Set.MakeSimpleSet[string]()
	for _, arg := range args[1:] {
		anoSet, errReply := db.GetSet(string(arg))
		if errReply != nil {
			return errReply
		}
		if anoSet == nil {
			continue
		}
		set = set.Union(anoSet)
	}
	if set.Len() == 0 {
		db.Remove(dest)
		db.ToAOF(utils.StringToCmd("Del", dest))
		return reply.MakeIntReply(0)
	}
	db.Remove(dest)
	db.ToAOF(utils.StringToCmd("Del", dest))
	db.Put(dest, _type.NewEntity(set))
	db.ToAOF(utils.StringToCmd("SAdd", set.Members()...))
	return reply.MakeIntReply(int64(set.Len()))
}

func execSDiffStore(db *redis.Database, args _type.Args) _interface.Reply {
	dest := string(args[0])
	set, errReply := db.GetSet(string(args[1]))
	if errReply != nil {
		return errReply
	}
	if set == nil {
		db.Remove(dest) // 清掉dest
		db.ToAOF(utils.StringToCmd("Del", dest))
		return reply.MakeIntReply(0)
	}
	// 求其余set的并集，然后在求差集，可提高效率
	unionSet := Set.MakeSimpleSet[string]()
	for _, arg := range args[2:] {
		anoSet, errReply := db.GetSet(string(arg))
		if errReply != nil {
			return errReply
		}
		if anoSet == nil {
			continue
		}
		unionSet = unionSet.Union(anoSet)
	}
	set = set.Diff(unionSet)
	if set.Len() == 0 {
		db.Remove(dest) // 清掉dest
		db.ToAOF(utils.StringToCmd("Del", dest))
		return reply.MakeIntReply(0)
	}
	db.Remove(dest) // 清掉dest
	db.ToAOF(utils.StringToCmd("Del", dest))
	db.Put(dest, _type.NewEntity(set))
	db.ToAOF(utils.StringToCmd("SAdd", set.Members()...))
	return reply.MakeIntReply(int64(set.Len()))
}

package redis

import (
	"fmt"
	Dict "go-redis/datastruct/dict"
	List "go-redis/datastruct/list"
	Set "go-redis/datastruct/set"
	ZSet "go-redis/datastruct/zset"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
	"go-redis/utils/logger"
	_sync "go-redis/utils/sync"
	"go-redis/utils/timewheel"
	"strings"
	"time"
)

const (
	dataSize   = 1 << 16
	ttlSize    = 1 << 10
	lockerSize = 1 << 10
)

type Database struct {
	idx  int                              // 数据库编号
	data Dict.Dict[string, *_type.Entity] // 数据
	ttl  Dict.Dict[string, time.Time]     // 超时时间
	//version Dict.Dict[string, uint32]        // 版本，用于事务
	locker *_sync.Locker       // 锁，用于执行命令时为key加锁
	ToAOF  func(_type.CmdLine) // 添加命令到aof
}

func MakeDatabase(idx int) *Database {
	database := &Database{
		idx:  idx,
		data: Dict.MakeConcurrentDict[string, *_type.Entity](dataSize),
		ttl:  Dict.MakeConcurrentDict[string, time.Time](ttlSize),
		//version: Dict.MakeConcurrentDict[string, uint32](dataSize),
		locker: _sync.MakeLocker(lockerSize),
		ToAOF:  func(line _type.CmdLine) {},
	}
	return database
}

func MakeSimpleDatabase(idx int) *Database {
	database := &Database{
		idx:  idx,
		data: Dict.MakeSimpleDict[string, *_type.Entity](),
		ttl:  Dict.MakeSimpleDict[string, time.Time](),
		//version: Dict.MakeSimpleDict[string, uint32](),
		locker: _sync.MakeLocker(1),
		ToAOF:  func(line _type.CmdLine) {},
	}
	return database
}

func (db *Database) Execute(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	return db.execCommand(cmdLine)
}

func (db *Database) execCommand(cmdLine _type.CmdLine) _interface.Reply {
	cmdName := strings.ToLower(string(cmdLine[0])) // 获取命令
	cmd, ok := Commands[cmdName]
	// 是否存在该命令
	if !ok {
		return Reply.MakeErrReply("unknown command '" + cmdName + "'")
	}
	// 参数个数是否满足要求
	if !checkArgNum(cmd.Arity, cmdLine) {
		return Reply.MakeArgNumErrReply(cmdName)
	}
	args := _type.Args(cmdLine[1:])           // 获取参数
	writeKeys, readKeys := cmd.keysFind(args) // 获取需要加锁的key
	//db.addVersion(writeKeys...)
	// 加锁
	db.lockKeys(writeKeys, readKeys)
	defer db.unLockKeys(writeKeys, readKeys)
	// 执行
	reply := cmd.Execute(db, args)
	return reply
}

// 检查参数个数是否满足要求
func checkArgNum(arity int, cmdLine _type.CmdLine) bool {
	argNum := len(cmdLine)
	if arity >= 0 {
		return argNum == arity
	}
	return argNum >= -arity
}

/* ----- Lock Keys----- */

func (db *Database) lockKeys(writeKeys []string, readKeys []string) {
	db.locker.LockKeys(writeKeys, readKeys)
}

func (db *Database) unLockKeys(writeKeys []string, readKeys []string) {
	db.locker.UnLockKeys(writeKeys, readKeys)
}

/* ----- Time To Live ----- */

func (db *Database) SetExpire(key string, expire time.Time) {
	db.ttl.Put(key, expire)
	taskKey := "expire:" + key
	// 创建定时任务
	timewheel.At(expire, taskKey, func() {
		keys := []string{key}
		// 上锁
		db.lockKeys(keys, nil)
		defer db.unLockKeys(keys, nil)
		logger.Info(fmt.Sprintf("key '%s' expired", key))
		expireTime, ok := db.ttl.Get(key)
		if !ok {
			return
		}
		// 由于过期时间可能被更新，故需要再次检查是否过期
		isExpired := time.Now().After(expireTime)
		if isExpired {
			db.Remove(key)
		}
	})
}

func (db *Database) Persist(key string) {
	db.ttl.Remove(key)
	taskKey := "expire:" + key
	timewheel.Cancel(taskKey)
}

func (db *Database) GetExpireTime(key string) (time.Time, bool) {
	expire, ok := db.ttl.Get(key)
	if !ok {
		return expire, false // 未设置过期时间
	}
	return expire, true
}

func (db *Database) IsExpired(key string) bool {
	expire, ok := db.ttl.Get(key)
	if !ok {
		return false // 未设置过期时间
	}
	// 再次检查是否过期
	isExpired := time.Now().After(expire)
	if isExpired {
		db.Remove(key) // 过期，移除key
	}
	return isExpired
}

/* ----- Entity Operation ----- */

func (db *Database) Get(key string) (*_type.Entity, bool) {
	entity, ok := db.data.Get(key)
	if !ok {
		return nil, false // key不存在
	}
	if db.IsExpired(key) {
		return nil, false // key已过期
	}
	return entity, true
}

func (db *Database) Put(key string, entity *_type.Entity) int {
	return db.data.Put(key, entity)
}

func (db *Database) PutIfExists(key string, entity *_type.Entity) int {
	return db.data.PutIfExists(key, entity)
}

func (db *Database) PutIfAbsent(key string, entity *_type.Entity) int {
	return db.data.PutIfAbsent(key, entity)
}

func (db *Database) Remove(key string) {
	db.data.Remove(key)
	db.ttl.Remove(key)
	taskKey := "expire:" + key
	timewheel.Cancel(taskKey)
}

func (db *Database) Removes(keys ...string) (count int) {
	count = 0
	for _, key := range keys {
		_, exists := db.data.Get(key)
		if exists {
			db.Remove(key)
			count++
		}
	}
	return count
}

func (db *Database) ForEach(operate func(key string, entity *_type.Entity, expire *time.Time) bool) {
	consumer := func(key string, entity *_type.Entity) bool {
		var expire *time.Time = nil
		expireTime, ok := db.ttl.Get(key)
		if ok {
			expire = &expireTime
		}
		return operate(key, entity, expire)
	}
	db.data.ForEach(consumer)
}

func (db *Database) Flush() {
	db.data.Clear()
	db.ttl.Clear()
	db.locker = _sync.MakeLocker(lockerSize) // 重置锁
}

/* ----- Get Entity ----- */

func (db *Database) GetString(key string) ([]byte, _interface.Reply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	bytes, ok := entity.Data.([]byte) // string的底层为[]byte，而非string
	if !ok {
		return nil, Reply.MakeWrongTypeErrReply()
	}
	return bytes, nil
}

func (db *Database) GetList(key string) (List.List[[]byte], _interface.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	list, ok := entity.Data.(List.List[[]byte])
	if !ok {
		return nil, Reply.MakeWrongTypeErrReply()
	}
	return list, nil
}

func (db *Database) GetSet(key string) (Set.Set[string], _interface.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	set, ok := entity.Data.(Set.Set[string])
	if !ok {
		return nil, Reply.MakeWrongTypeErrReply()
	}
	return set, nil
}

func (db *Database) GetZSet(key string) (ZSet.ZSet[string], _interface.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	zset, ok := entity.Data.(ZSet.ZSet[string])
	if !ok {
		return nil, Reply.MakeWrongTypeErrReply()
	}
	return zset, nil
}

func (db *Database) GetDict(key string) (Dict.Dict[string, []byte], _interface.ErrorReply) {
	entity, exists := db.Get(key)
	if !exists {
		return nil, nil
	}
	dict, ok := entity.Data.(Dict.Dict[string, []byte])
	if !ok {
		return nil, Reply.MakeWrongTypeErrReply()
	}
	return dict, nil
}

/* ----- Get or Init Entity ----- */

func (db *Database) GetOrInitList(key string) (list List.List[[]byte], isNew bool, errReply _interface.ErrorReply) {
	list, errReply = db.GetList(key)
	if errReply != nil {
		return nil, false, errReply // WrongTypeErrReply
	}
	isNew = false
	if list == nil {
		// 初始化list
		list = List.MakeQuickList[[]byte]() // list由[]byte类型的QuickList实现
		entity := _type.NewEntity(list)
		db.Put(key, entity)
		isNew = true
	}
	return list, isNew, nil
}

func (db *Database) GetOrInitSet(key string) (set Set.Set[string], isNew bool, errReply _interface.ErrorReply) {
	set, errReply = db.GetSet(key)
	if errReply != nil {
		return nil, false, errReply // WrongTypeErrReply
	}
	isNew = false
	if set == nil {
		// 初始化set
		set = Set.MakeSimpleSet[string]() // set由string类型的SimpleSet实现
		entity := _type.NewEntity(set)
		db.Put(key, entity)
		isNew = true
	}
	return set, isNew, nil
}

func (db *Database) GetOrInitZSet(key string) (zset ZSet.ZSet[string], isNew bool, errReply _interface.ErrorReply) {
	zset, errReply = db.GetZSet(key)
	if errReply != nil {
		return nil, false, errReply // WrongTypeErrReply
	}
	isNew = false
	if zset == nil {
		// 初始化zset
		compare := func(a string, b string) int {
			if a < b {
				return -1
			} else if a > b {
				return 1
			} else {
				return 0
			}
		}
		zset = ZSet.MakeSortedSet[string](compare) // zest由string类型的SortedSet实现
		entity := _type.NewEntity(zset)
		db.Put(key, entity)
		isNew = true
	}
	return zset, isNew, nil
}

func (db *Database) GetOrInitDict(key string) (set Dict.Dict[string, []byte], isNew bool, errReply _interface.ErrorReply) {
	set, errReply = db.GetDict(key)
	if errReply != nil {
		return nil, false, errReply // WrongTypeErrReply
	}
	isNew = false
	if set == nil {
		// 初始化set
		set = Dict.MakeSimpleDict[string, []byte]() // hash由[string, []byte]类型的SimpleDict实现
		entity := _type.NewEntity(set)
		db.Put(key, entity)
		isNew = true
	}
	return set, isNew, nil
}

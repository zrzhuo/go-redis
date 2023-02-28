package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	"go-redis/redis/utils"
	Reply "go-redis/resp/reply"
	"strconv"
	"strings"
	"time"
)

func init() {
	RegisterSysCommand("sleep", execSleep, 2) // sleep，用于测试
	RegisterSysCommand("ping", execPing, -1)
	RegisterSysCommand("config", execConfig, -2)
	RegisterSysCommand("select", execSelect, 2)
	RegisterSysCommand("flushdb", execFlushDB, 1)
	RegisterSysCommand("flushall", execFlushAll, 1)
	RegisterSysCommand("subscribe", execSubscribe, -2)
	RegisterSysCommand("unsubscribe", execUnSubscribe, -2)
	RegisterSysCommand("publish", execPublish, -2)
	RegisterSysCommand("rewriteaof", execReWriteAOF, 1)     // aof重写
	RegisterSysCommand("bgrewriteaof", execBGReWriteAOF, 1) // 异步aof重写
	RegisterSysCommand("multi", execMulti, 1)               // 开启事务
	RegisterSysCommand("exec", execExec, 1)                 // 执行事务
	RegisterSysCommand("discard", execDiscard, 1)           // 退出事务
}

func execSleep(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	st, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return Reply.MakeErrReply("illegal integer.")
	}
	time.Sleep(time.Duration(st) * time.Second)
	return Reply.MakeStatusReply("sleep over")

}

func execPing(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	size := len(args)
	if size == 0 {
		return Reply.MakePongReply()
	}
	if size == 1 {
		return Reply.MakeStatusReply(string(args[0]))
	}
	return Reply.MakeArgNumErrReply("Ping")
}

func execConfig(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	num := len(args)
	if num == 0 {
		return Reply.MakeArgNumErrReply("config")
	}
	cmd := strings.ToLower(string(args[0]))
	if cmd == "get" {
		return execConfigGet(server, client, args)
	}
	if cmd == "set" {
		return execConfigSet(server, client, args)
	}
	return Reply.MakeErrReply(fmt.Sprintf("unknown subcommand '%s'", cmd))
}

func execConfigGet(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	num := len(args)
	if num < 2 {
		return Reply.MakeArgNumErrReply("config|get")
	}
	var result []string
	for i := 1; i < num; i++ {
		key := strings.ToLower(string(args[i]))
		val, ok := GetConfig(key)
		if !ok {
			continue
		}
		result = append(result, key)
		result = append(result, val)
	}
	return Reply.ToArrayReply(result...)
}

func execConfigSet(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	num := len(args)
	if num < 2 || num%2 == 0 {
		return Reply.MakeArgNumErrReply("config|set")
	}
	for i := 0; i < num/2; i++ {
		key := strings.ToLower(string(args[2*i+1]))
		val := strings.ToLower(string(args[2*i+2]))
		err := SetConfig(key, val)
		if err != nil {
			return Reply.MakeErrReply(err.Error())
		}
	}
	return Reply.MakeOkReply()
}

func execAuth(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) != 1 {
		return Reply.MakeArgNumErrReply("auth")
	}
	if Config.Requirepass == "" {
		return Reply.MakeErrReply("no password is set.")
	}
	password := string(args[0])
	client.SetPassword(password)
	if password != Config.Requirepass {
		return Reply.MakeErrReply("invalid password.")
	}
	return Reply.MakeOkReply()
}

func execSelect(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	dbIdx, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return Reply.MakeErrReply("selected index is invalid")
	}
	if dbIdx >= len(server.databases) || dbIdx < 0 {
		msg := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.MakeErrReply(msg)
	}
	client.SetSelectDB(dbIdx) // 修改client的dbIdx
	return Reply.MakeOkReply()
}

func execFlushDB(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	dbIdx := client.GetSelectDB()
	if dbIdx < 0 || dbIdx >= len(server.databases) {
		err := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.MakeErrReply(err)
	}
	db := server.databases[dbIdx].Load().(*Database)
	db.Flush()
	db.ToAOF(utils.ToCmd("flushdb", []byte(strconv.Itoa(dbIdx))))
	return Reply.MakeOkReply()
}

func execFlushAll(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	for i := 0; i < len(server.databases); i++ {
		db := server.databases[i].Load().(*Database)
		db.Flush()
		if i == 0 {
			db.ToAOF(utils.ToCmd("flushall"))
		}
	}
	return Reply.MakeOkReply()
}

func execSubscribe(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) < 1 {
		return Reply.MakeArgNumErrReply("subscribe")
	}
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	return server.pubsub.Subscribe(client, channels)
}

func execUnSubscribe(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	var channels []string
	if len(args) == 0 {
		channels = client.GetChannels() // 所有的channel
	} else {
		channels = make([]string, len(args)) // 指定的channel
		for i, arg := range args {
			channels[i] = string(arg)
		}
	}
	return server.pubsub.UnSubscribe(client, channels)
}

func execPublish(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) != 2 {
		return Reply.MakeArgNumErrReply("publish")
	}
	channel, message := string(args[0]), args[1]
	return server.pubsub.Publish(client, channel, message)
}

func execReWriteAOF(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	err := server.persister.ReWrite()
	if err != nil {
		return Reply.MakeErrReply(err.Error())
	}
	return Reply.MakeOkReply()
}

func execBGReWriteAOF(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	go func() {
		_ = server.persister.ReWrite()
	}()
	return Reply.MakeStatusReply("background aof rewriting started")
}

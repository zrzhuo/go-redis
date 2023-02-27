package redis

import (
	"fmt"
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
	"strconv"
)

func (server *Server) execCommand(client _interface.Client, cmdLine _type.CmdLine) _interface.Reply {
	dbIdx := client.GetSelectDB()
	if dbIdx < 0 || dbIdx >= len(server.databases) {
		err := fmt.Sprintf("selected index is out of range[0, %d]", len(server.databases)-1)
		return Reply.MakeErrReply(err)
	}
	db := server.databases[dbIdx].Load().(*Database)
	return db.Execute(client, cmdLine)
}

func (server *Server) execSubscribe(client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) < 1 {
		return Reply.MakeArgNumErrReply("subscribe")
	}
	channels := make([]string, len(args))
	for i, arg := range args {
		channels[i] = string(arg)
	}
	return server.pubsub.Subscribe(client, channels)
}

func (server *Server) execUnSubscribe(client _interface.Client, args _type.Args) _interface.Reply {
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

func (server *Server) execPublish(client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) != 2 {
		return Reply.MakeArgNumErrReply("publish")
	}
	channel, message := string(args[0]), args[1]
	return server.pubsub.Publish(client, channel, message)
}

func (server *Server) execPing(client _interface.Client, args _type.Args) _interface.Reply {
	size := len(args)
	if size == 0 {
		return Reply.MakePongReply()
	} else if size == 1 {
		return Reply.MakeStatusReply(string(args[0]))
	} else {
		return Reply.MakeArgNumErrReply("Ping")
	}
}

func (server *Server) execAuth(client _interface.Client, args _type.Args) _interface.Reply {
	if len(args) != 1 {
		return Reply.MakeArgNumErrReply("auth")
	}
	if Config.RequirePass == "" {
		return Reply.MakeErrReply("no password is set.")
	}
	password := string(args[0])
	client.SetPassword(password)
	if password != Config.RequirePass {
		return Reply.MakeErrReply("invalid password.")
	}
	return Reply.MakeOkReply()
}

func (server *Server) execSelect(client _interface.Client, args _type.Args) _interface.Reply {
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

func (server *Server) execReWriteAof(client _interface.Client, args _type.Args) _interface.Reply {
	err := server.persister.ReWrite()
	if err != nil {
		return Reply.MakeErrReply(err.Error())
	}
	return Reply.MakeOkReply()
}

func (server *Server) execBGReWriteAof(client _interface.Client, args _type.Args) _interface.Reply {
	go func() {
		_ = server.persister.ReWrite()
	}()
	return Reply.MakeStatusReply("background aof rewriting started")
}

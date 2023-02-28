package redis

import (
	_interface "go-redis/interface"
	_type "go-redis/interface/type"
	Reply "go-redis/resp/reply"
)

func execMulti(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if client.IsTxState() {
		return Reply.MakeErrReply("MULTI calls can not be nested")
	}
	client.SetTxState(true)
	return Reply.MakeOkReply()
}

func execExec(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if !client.IsTxState() {
		return Reply.MakeErrReply("EXEC without MULTI")
	}
	defer client.SetTxState(false) // 解除事务状态
	if len(client.GetTxError()) > 0 {
		return Reply.MakeErrReply("EXECABORT Transaction discarded because of previous errors.")
	}
	cmdLines := client.GetTxQueue()
	replies := make([]string, 0, len(cmdLines))
	for _, cmdLine := range cmdLines {
		reply := server.ExecWithoutLock(client, cmdLine)
		if Reply.IsErrorReply(reply) {
			break
		}
		replies = append(replies, string(reply.ToBytes()))
	}
	return Reply.ToArrayReply(replies...)
}
func execDiscard(server *Server, client _interface.Client, args _type.Args) _interface.Reply {
	if !client.IsTxState() {
		return Reply.MakeErrReply("DISCARD without MULTI")
	}
	client.SetTxState(false)
	client.ClearTxQueue()
	return Reply.MakeOkReply()
}

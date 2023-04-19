package reply

import _interface "go-redis/interface"

func StringToBulkReply(arg string) *BulkReply {
	return &BulkReply{
		Bulk: []byte(arg),
	}
}

func StringToArrayReply(lines ...string) *ArrayReply {
	args := make([][]byte, len(lines))
	for i, line := range lines {
		args[i] = []byte(line)
	}
	return NewArrayReply(args)
}

func IsOKReply(reply _interface.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

func IsErrorReply(reply _interface.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

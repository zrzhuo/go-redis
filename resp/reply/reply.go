package reply

import (
	"bytes"
	"go-redis/interface"
	"strconv"
)

var CRLF = "\r\n" // RESP定义的换行符

/* ---- Bulk String ---- */

type BulkReply struct {
	Arg []byte
}

func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{
		Arg: arg,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Arg == nil {
		return nullBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Arg)) + CRLF + string(r.Arg) + CRLF)
}

/* ---- Array (Multi Bulk Strings) ---- */

type ArrayReply struct {
	Args [][]byte
}

func MakeArrayReply(args [][]byte) *ArrayReply {
	return &ArrayReply{
		Args: args,
	}
}

func StringToArrayReply(lines ...string) *ArrayReply {
	args := make([][]byte, len(lines))
	for i, line := range lines {
		args[i] = []byte(line)
	}
	return MakeArrayReply(args)
}

func (r *ArrayReply) ToBytes() []byte {
	length := len(r.Args)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(length) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/* ---- Multi Raw Reply ---- */

// MultiRawReply store complex list structure, for example GeoPos commands
type MultiRawReply struct {
	Replies []_interface.Reply
}

func MakeMultiRawReply(replies []_interface.Reply) *MultiRawReply {
	return &MultiRawReply{
		Replies: replies,
	}
}

func (r *MultiRawReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Replies {
		buf.Write(arg.ToBytes())
	}
	return buf.Bytes()
}

/* ---- Status ---- */

type StatusReply struct {
	Status string
}

func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/* ---- Int Reply ---- */

type IntReply struct {
	Code int64
}

func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

/* ---- Others ---- */

func IsOKReply(reply _interface.Reply) bool {
	return string(reply.ToBytes()) == "+OK\r\n"
}

func IsErrorReply(reply _interface.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

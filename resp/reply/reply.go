package reply

import (
	"bytes"
	_interface "go-redis/interface"
	"strconv"
)

var CRLF = "\r\n" // RESP定义的换行符

/* ---- String Reply ---- */

type StringReply struct {
	Content string
}

func NewStringReply(content string) *StringReply {
	return &StringReply{
		Content: content,
	}
}

func (r *StringReply) ToBytes() []byte {
	return []byte("+" + r.Content + CRLF)
}

/* ---- Error Reply ---- */

type ErrorReply struct {
	Error string
}

func NewErrorReply(err string) *ErrorReply {
	return &ErrorReply{
		Error: err,
	}
}

func (r *ErrorReply) ToBytes() []byte {
	return []byte("-ERR: " + r.Error + "\r\n")
}

/* ---- Integer Reply ---- */

type IntegerReply struct {
	Integer int64
}

func NewIntegerReply(integer int64) *IntegerReply {
	return &IntegerReply{
		Integer: integer,
	}
}

func (r *IntegerReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Integer, 10) + CRLF)
}

/* ---- Bulk String Reply---- */

type BulkReply struct {
	Bulk []byte
}

func NewBulkReply(bulk []byte) *BulkReply {
	return &BulkReply{
		Bulk: bulk,
	}
}

func (r *BulkReply) ToBytes() []byte {
	if r.Bulk == nil {
		return nilBulkBytes
	}
	return []byte("$" + strconv.Itoa(len(r.Bulk)) + CRLF + string(r.Bulk) + CRLF)
}

/* ---- Array Reply (multi bulk strings) ---- */

type ArrayReply struct {
	Bulks [][]byte
}

func NewArrayReply(bulks [][]byte) *ArrayReply {
	return &ArrayReply{
		Bulks: bulks,
	}
}

func (r *ArrayReply) ToBytes() []byte {
	length := len(r.Bulks)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(length) + CRLF)
	for _, arg := range r.Bulks {
		if arg == nil {
			buf.Write(nilBulkBytes)
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}
	return buf.Bytes()
}

/* ---- Raw Array Reply (array of reply) ---- */

type RawArrayReply struct {
	Replies []_interface.Reply
}

func NewRawArrayReply(replies []_interface.Reply) *RawArrayReply {
	return &RawArrayReply{
		Replies: replies,
	}
}

func (r *RawArrayReply) ToBytes() []byte {
	argLen := len(r.Replies)
	var buf bytes.Buffer
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, reply := range r.Replies {
		buf.Write(reply.ToBytes())
	}
	return buf.Bytes()
}

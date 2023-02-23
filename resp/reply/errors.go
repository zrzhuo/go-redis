package reply

/* ---- Standard Error Reply ---- */

type StandardErrReply struct {
	Status string
}

func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-ERR: " + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

/* ---- Unknown Error Reply ---- */

type UnknownErrReply struct{}

var unknownErrReply = &UnknownErrReply{}

var unknownErrBytes = []byte("-ERR: unknown" + CRLF)

func MakeUnknownErrReply() *UnknownErrReply {
	return unknownErrReply
}

func (r *UnknownErrReply) ToBytes() []byte {
	return unknownErrBytes
}

func (r *UnknownErrReply) Error() string {
	return "unknown error occurred"
}

/* ---- ArgNum Error Reply ---- */

type ArgNumErrReply struct {
	Cmd string
}

func MakeArgNumErrReply(cmd string) *ArgNumErrReply {
	return &ArgNumErrReply{
		Cmd: cmd,
	}
}

func (r *ArgNumErrReply) ToBytes() []byte {
	return []byte("-ERR: wrong number of arguments in '" + r.Cmd + "'" + CRLF)
}

func (r *ArgNumErrReply) Error() string {
	return "wrong number of arguments in '" + r.Cmd + "'"
}

/* ---- Syntax Error Reply ---- */

type SyntaxErrReply struct{}

var syntaxErrBytes = []byte("-ERR: syntax error" + CRLF)

func MakeSyntaxErrReply() *SyntaxErrReply {
	return &SyntaxErrReply{}
}

func (r *SyntaxErrReply) ToBytes() []byte {
	return syntaxErrBytes
}

func (r *SyntaxErrReply) Error() string {
	return "syntax error"
}

/* ---- WrongType Error Reply ---- */

type WrongTypeErrReply struct{}

var wrongTypeErrBytes = []byte("-ERR: wrong type of value" + CRLF)

func MakeWrongTypeErrReply() *WrongTypeErrReply {
	return &WrongTypeErrReply{}
}

func (r *WrongTypeErrReply) ToBytes() []byte {
	return wrongTypeErrBytes
}

func (r *WrongTypeErrReply) Error() string {
	return "wrong type of value"
}

/* ---- Protocol Error Reply ---- */

// ProtocolErrReply represents meeting unexpected byte during parse requests
type ProtocolErrReply struct {
	Msg string
}

func MakeProtocolErrReply(msg string) *ProtocolErrReply {
	return &ProtocolErrReply{
		Msg: msg,
	}
}

func (r *ProtocolErrReply) ToBytes() []byte {
	return []byte("-ERR: protocol error, '" + r.Msg + "'" + CRLF)
}

func (r *ProtocolErrReply) Error() string {
	return "protocol error: '" + r.Msg
}

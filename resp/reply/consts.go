package reply

/* ---- Pong Reply ---- */

type PongReply struct{}

var pongReply = &PongReply{}

var pongBytes = []byte("+PONG" + CRLF)

func MakePongReply() *PongReply {
	return pongReply
}

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

/* ---- Ok Reply ---- */

type OkReply struct{}

var okReply = &OkReply{}

var okBytes = []byte("+OK" + CRLF)

func MakeOkReply() *OkReply {
	return okReply
}

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

/* ---- Nil Bulk String Reply ---- */

type NilBulkReply struct{}

var nilBulkReply = &NilBulkReply{}

var nilBulkBytes = []byte("$-1" + CRLF)

func MakeNilBulkReply() *NilBulkReply {
	return nilBulkReply
}

func (r *NilBulkReply) ToBytes() []byte {
	return nilBulkBytes
}

/* ---- Empty Bulk String Reply ---- */

type NoReply struct{}

var noReply = &NoReply{}

var noBytes = []byte("")

func MakeEmptyBulkReply() *NoReply {
	return noReply
}

func (r *NoReply) ToBytes() []byte {
	return noBytes
}

/* ---- Empty Array Reply ---- */

type EmptyArrayReply struct{}

var emptyArrayReply = &EmptyArrayReply{}

var emptyArrayBytes = []byte("*0" + CRLF)

func MakeEmptyArrayReply() *EmptyArrayReply {
	return emptyArrayReply
}

func (r *EmptyArrayReply) ToBytes() []byte {
	return emptyArrayBytes
}

/* ---- Queued Reply ---- */

type QueuedReply struct{}

var queuedReply = &QueuedReply{}

var queuedBytes = []byte("+QUEUED" + CRLF)

func MakeQueuedReply() *QueuedReply {
	return queuedReply
}

func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

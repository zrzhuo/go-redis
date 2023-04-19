package reply

/* ---- Pong Reply ---- */

type PongReply struct{}

var pongReply = &PongReply{}

var pongBytes = []byte("+PONG\r\n")

func NewPongReply() *PongReply {
	return pongReply
}

func (r *PongReply) ToBytes() []byte {
	return pongBytes
}

/* ---- Ok Reply ---- */

type OkReply struct{}

var okReply = &OkReply{}

var okBytes = []byte("+OK\r\n")

func NewOkReply() *OkReply {
	return okReply
}

func (r *OkReply) ToBytes() []byte {
	return okBytes
}

/* ---- Nil Bulk String Reply ---- */

type NilBulkReply struct{}

var nilBulkReply = &NilBulkReply{}

var nilBulkBytes = []byte("$-1\r\n")

func NewNilBulkReply() *NilBulkReply {
	return nilBulkReply
}

func (r *NilBulkReply) ToBytes() []byte {
	return nilBulkBytes
}

/* ---- Empty Bulk String Reply ---- */

type EmptyReply struct{}

var emptyReply = &EmptyReply{}

var emptyBytes = []byte("$0\r\n\r\n")

func NewEmptyBulkReply() *EmptyReply {
	return emptyReply
}

func (r *EmptyReply) ToBytes() []byte {
	return emptyBytes
}

/* ---- Empty Array Reply ---- */

type EmptyArrayReply struct{}

var emptyArrayReply = &EmptyArrayReply{}

var emptyArrayBytes = []byte("*0\r\n")

func NewEmptyArrayReply() *EmptyArrayReply {
	return emptyArrayReply
}

func (r *EmptyArrayReply) ToBytes() []byte {
	return emptyArrayBytes
}

/* ---- Queued Reply ---- */

type QueuedReply struct{}

var queuedReply = &QueuedReply{}

var queuedBytes = []byte("+QUEUED\r\n")

func NewQueuedReply() *QueuedReply {
	return queuedReply
}

func (r *QueuedReply) ToBytes() []byte {
	return queuedBytes
}

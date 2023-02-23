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

/* ---- Null Bulk String Reply ---- */

type NullBulkReply struct{}

var nullBulkReply = &NullBulkReply{}

var nullBulkBytes = []byte("$-1" + CRLF)

func MakeNullBulkReply() *NullBulkReply {
	return nullBulkReply
}

func (r *NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}

/* ---- Empty Multi Bulk Strings Reply ---- */

type EmptyMultiBulkReply struct{}

var emptyMultiBulkReply = &EmptyMultiBulkReply{}

var emptyMultiBulkBytes = []byte("*0" + CRLF)

func MakeEmptyMultiBulkReply() *EmptyMultiBulkReply {
	return emptyMultiBulkReply
}

func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

/* ---- Empty Multi Bulk Strings Reply ---- */

type NoReply struct{}

var noReply = &NoReply{}

var noBytes = []byte("")

func MakeNoReply() *NoReply {
	return noReply
}

func (r *NoReply) ToBytes() []byte {
	return noBytes
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
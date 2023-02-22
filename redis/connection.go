package redis

import (
	"go-redis/utils/logger"
	sync2 "go-redis/utils/sync"
	"net"
	"sync"
	"time"
)

const (
	flagSlave  = uint64(1 << iota) // means this a connection with slave
	flagMaster                     // means this a connection with master
	flagMulti                      // means this connection is within a transaction
)

type Connection struct {
	Conn       net.Conn
	flags      uint64
	selectedDB int
	wait       sync2.Wait      // wait until finish sending data, used for graceful shutdown
	mu         sync.Mutex      // lock while engine sending response
	subs       map[string]bool // subscribing channels
	password   string          // password may be changed by CONFIG commands during runtime, so store the password
	queue      [][][]byte      // queued commands for `multi`
	watching   map[string]uint32
	txErrors   []error
}

// 连接池
var connPool = sync.Pool{
	New: func() any {
		return &Connection{}
	},
}

func NewRedisConn(tcpConn net.Conn) *Connection {
	redisConn, ok := connPool.Get().(*Connection) // 尝试从连接池中获取连接
	if !ok {
		logger.Error("wrong connection type")
		// 从连接池中获取失败，新建一个
		return &Connection{
			Conn: tcpConn,
		}
	}
	redisConn.Conn = tcpConn
	return redisConn
}

// GetAofConn 用于aof
func GetAofConn() *Connection {
	return &Connection{}
}

// Write 项客户端发送响应
func (c *Connection) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	c.wait.Add(1) // 加入等待组
	defer func() {
		c.wait.Done()
	}()
	return c.Conn.Write(b)
}

// Close 关闭连接
func (c *Connection) Close() error {
	c.wait.WaitWithTimeout(10 * time.Second) // 等待执行结束或超时
	_ = c.Conn.Close()
	c.subs = nil
	c.password = ""
	c.queue = nil
	c.watching = nil
	c.txErrors = nil
	c.selectedDB = 0
	connPool.Put(c) // 将连接放回连接池
	return nil
}

func (c *Connection) RemoteAddr() string {
	if c.Conn != nil {
		return c.Conn.RemoteAddr().String()
	}
	return ""
}

func (c *Connection) GetSelectDB() int {
	return c.selectedDB
}

func (c *Connection) SetSelectDB(n int) {
	c.selectedDB = n
}

/* ---- authentication ---- */

// SetPassword stores password for authentication
func (c *Connection) SetPassword(password string) {
	c.password = password
}

// GetPassword get password for authentication
func (c *Connection) GetPassword() string {
	return c.password
}

/* ---- subscribe ---- */

// Subscribe add current connection into subscribers of the given channel
func (c *Connection) Subscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.subs == nil {
		c.subs = make(map[string]bool)
	}
	c.subs[channel] = true
}

// UnSubscribe removes current connection into subscribers of the given channel
func (c *Connection) UnSubscribe(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.subs) == 0 {
		return
	}
	delete(c.subs, channel)
}

// SubsCount returns the number of subscribing channels
func (c *Connection) SubsCount() int {
	return len(c.subs)
}

// GetChannels returns all subscribing channels
func (c *Connection) GetChannels() []string {
	if c.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(c.subs))
	i := 0
	for channel := range c.subs {
		channels[i] = channel
		i++
	}
	return channels
}

/* ---- transaction ---- */

// InMultiState tells is connection in an uncommitted transaction
func (c *Connection) InMultiState() bool {
	return c.flags&flagMulti > 0
}

// SetMultiState sets transaction flag
func (c *Connection) SetMultiState(state bool) {
	if !state { // reset data when cancel multi
		c.watching = nil
		c.queue = nil
		c.flags &= ^flagMulti // clean multi flag
		return
	}
	c.flags |= flagMulti
}

// GetQueuedCmdLine returns queued commands of current transaction
func (c *Connection) GetQueuedCmdLine() [][][]byte {
	return c.queue
}

// EnqueueCmd  enqueues commands of current transaction
func (c *Connection) EnqueueCmd(cmdLine [][]byte) {
	c.queue = append(c.queue, cmdLine)
}

// AddTxError stores syntax error within transaction
func (c *Connection) AddTxError(err error) {
	c.txErrors = append(c.txErrors, err)
}

// GetTxErrors returns syntax error within transaction
func (c *Connection) GetTxErrors() []error {
	return c.txErrors
}

// ClearQueuedCmds clears queued commands of current transaction
func (c *Connection) ClearQueuedCmds() {
	c.queue = nil
}

// GetWatching returns watching keys and their version code when started watching
func (c *Connection) GetWatching() map[string]uint32 {
	if c.watching == nil {
		c.watching = make(map[string]uint32)
	}
	return c.watching
}

/* ---- flags ---- */

func (c *Connection) SetSlave() {
	c.flags |= flagSlave
}

func (c *Connection) IsSlave() bool {
	return c.flags&flagSlave > 0
}

func (c *Connection) SetMaster() {
	c.flags |= flagMaster
}

func (c *Connection) IsMaster() bool {
	return c.flags&flagMaster > 0
}

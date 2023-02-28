package redis

import (
	_type "go-redis/interface/type"
	_sync "go-redis/utils/sync"
	"net"
	"sync"
	"time"
)

type Client struct {
	conn       net.Conn
	selectedDB int    // 选择的数据库id
	password   string // 密码
	wait       _sync.Wait

	// 发布订阅
	channels map[string]bool // 当前订阅的channel
	locker   sync.Mutex      // 锁

	// 事务
	txState bool              // 事务状态
	txQueue []_type.CmdLine   // 命令队列
	txError []error           // 错误
	txWatch map[string]uint32 // watch
}

/* ---- transaction ---- */

func (client *Client) IsTxState() bool {
	return client.txState
}

func (client *Client) SetTxState(flag bool) {
	client.txState = flag
}

func (client *Client) EnTxQueue(cmdLine _type.CmdLine) {
	client.txQueue = append(client.txQueue, cmdLine)
}

func (client *Client) GetTxQueue() []_type.CmdLine {
	return client.txQueue
}
func (client *Client) ClearTxQueue() {
	client.txQueue = nil
}

func (client *Client) AddTxError(err error) {
	client.txError = append(client.txError, err)
}

func (client *Client) GetTxError() []error {
	return client.txError
}

// 连接池
var clientPool = sync.Pool{
	New: func() any {
		return &Client{}
	},
}

func NewClient(conn net.Conn) *Client {
	client, ok := clientPool.Get().(*Client) // 尝试从连接池中获取
	if !ok {
		// 从连接池中获取失败，新建一个
		return &Client{
			conn: conn,
		}
	}
	client.conn = conn
	return client
}

// GetAofClient 用于aof
func GetAofClient() *Client {
	return &Client{}
}

func (client *Client) Write(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, nil
	}
	client.wait.Add(1) // 加入等待组
	defer func() {
		client.wait.Done()
	}()
	return client.conn.Write(b)
}

func (client *Client) Close() error {
	client.wait.WaitWithTimeout(10 * time.Second) // 等待执行结束或超时
	err := client.conn.Close()
	if err != nil {
		return err
	}
	client.selectedDB = 0
	client.password = ""
	clientPool.Put(client) // 放回连接池
	return nil
}

func (client *Client) RemoteAddr() string {
	if client.conn != nil {
		return client.conn.RemoteAddr().String()
	}
	return ""
}

/* ---- select db ---- */

func (client *Client) GetSelectDB() int {
	return client.selectedDB
}

func (client *Client) SetSelectDB(n int) {
	client.selectedDB = n
}

/* ---- authentication ---- */

func (client *Client) SetPassword(password string) {
	client.password = password
}

func (client *Client) GetPassword() string {
	return client.password
}

/* ---- publish/subscribe ---- */

func (client *Client) Subscribe(channel string) {
	client.locker.Lock() // 上锁
	defer client.locker.Unlock()
	if client.channels == nil {
		client.channels = make(map[string]bool)
	}
	client.channels[channel] = true
}

func (client *Client) UnSubscribe(channel string) {
	client.locker.Lock() // 上锁
	defer client.locker.Unlock()
	if client.channels == nil {
		return
	}
	delete(client.channels, channel)
}

func (client *Client) ChannelsCount() int {
	return len(client.channels)
}

func (client *Client) GetChannels() []string {
	if client.channels == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(client.channels))
	i := 0
	for channel := range client.channels {
		channels[i] = channel
		i++
	}
	return channels
}

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
	selectedDB int        // 选择的数据库id
	password   string     // 密码
	wait       _sync.Wait // 等待数据发送完毕

	// 发布订阅
	channels map[string]bool // 当前订阅的channel
	subLock  sync.Mutex      // sub/unsub时的锁

	// 事务
	txState bool             // 事务状态
	txQueue []_type.CmdLine  // 命令队列
	txError []error          // 错误
	txWatch []map[string]int // watch的key
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
		client = &Client{} // 从连接池中获取失败，新建一个
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
	// 初始化该client，并放回连接池
	client.wait.WaitWithTimeout(10 * time.Second) // 等待执行结束或超时
	err := client.conn.Close()
	if err != nil {
		return err
	}
	client.selectedDB = 0
	client.password = ""
	client.channels = nil
	client.txState = false
	client.txQueue = nil
	client.txError = nil
	client.txWatch = nil
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
	client.subLock.Lock() // 上锁
	defer client.subLock.Unlock()
	if client.channels == nil {
		client.channels = make(map[string]bool)
	}
	client.channels[channel] = true
}

func (client *Client) UnSubscribe(channel string) {
	client.subLock.Lock() // 上锁
	defer client.subLock.Unlock()
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

func (client *Client) InitWatch(dbNum int) {
	if client.txWatch != nil {
		return
	}
	txWatch := make([]map[string]int, dbNum)
	for i := 0; i < dbNum; i++ {
		txWatch[i] = make(map[string]int)
	}
	client.txWatch = txWatch
}

func (client *Client) DestoryWatch() {
	client.txWatch = nil
}

func (client *Client) SetWatchKey(dbIdx int, key string, version int) {
	client.txWatch[dbIdx][key] = version
}

func (client *Client) GetWatchKeys() []map[string]int {
	return client.txWatch
}

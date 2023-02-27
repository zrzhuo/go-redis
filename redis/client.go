package redis

import (
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
	mu         sync.Mutex
	subs       map[string]bool
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
	client.mu.Lock() // 上锁
	defer client.mu.Unlock()
	if client.subs == nil {
		client.subs = make(map[string]bool)
	}
	client.subs[channel] = true
}

func (client *Client) UnSubscribe(channel string) {
	client.mu.Lock() // 上锁
	defer client.mu.Unlock()
	if client.subs == nil {
		return
	}
	delete(client.subs, channel)
}

func (client *Client) SubsCount() int {
	return len(client.subs)
}

func (client *Client) GetChannels() []string {
	if client.subs == nil {
		return make([]string, 0)
	}
	channels := make([]string, len(client.subs))
	i := 0
	for channel := range client.subs {
		channels[i] = channel
		i++
	}
	return channels
}

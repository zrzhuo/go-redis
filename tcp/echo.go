package tcp

import (
	"bufio"
	"go-redis/utils/logger"
	sync2 "go-redis/utils/sync"
	"io"
	"net"
	"sync"
	"time"
)

type EchoConn struct {
	Conn net.Conn   // tcp连接
	Wait sync2.Wait // 当服务端开始发送数据时进入waiting, 阻止其它goroutine关闭连接
}

func (c *EchoConn) Close() error {
	c.Wait.WaitWithTimeout(10 * time.Second) // 等待数据发送完成或超时
	_ = c.Conn.Close()                       // 关闭连接
	return nil
}

// EchoHandler echos received line to client, using for test
type EchoHandler struct {
	activeConn sync.Map      // 存放活动中的连接
	isClosing  sync2.Boolean // 当前handler是否正处于closing状态
}

func MakeEchoHandler() *EchoHandler {
	return &EchoHandler{}
}

func (handler *EchoHandler) Handle(conn net.Conn) {
	if handler.isClosing.Get() {
		_ = conn.Close() // handler正处于closing状态，拒绝该连接
		return
	}
	// 建立EchoConn，并存入activeConn
	echoConn := &EchoConn{
		Conn: conn,
	}
	handler.activeConn.Store(echoConn, struct{}{})
	// handle
	reader := bufio.NewReader(conn)
	for {
		// 读取一行输入
		msg, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				logger.Info("connection close")
				handler.activeConn.Delete(echoConn) // io.EOF
			} else {
				logger.Warn(err)
			}
			return
		}
		// 将该输入作为输出返回
		echoConn.Wait.Add(1)
		b := []byte(msg)
		_, _ = conn.Write(b)
		echoConn.Wait.Done()
	}
}

func (handler *EchoHandler) Close() error {
	logger.Info("handler shutting down...")
	handler.isClosing.Set(true) // 设置closing为true
	// 关闭activeConn中的每一个连接
	handler.activeConn.Range(func(key interface{}, val interface{}) bool {
		client := key.(*EchoConn)
		_ = client.Close()
		return true
	})
	return nil
}

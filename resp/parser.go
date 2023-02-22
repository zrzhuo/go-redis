package resp

import (
	"bufio"
	"bytes"
	"errors"
	reply3 "go-redis/resp/reply"
	"go-redis/utils/logger"
	"io"
	"runtime/debug"
	"strconv"
)

type Parser struct {
	reader *bufio.Reader
	ch     chan *Payload
}

func MakeParser(reader io.Reader) *Parser {
	return &Parser{
		reader: bufio.NewReader(reader),
		ch:     make(chan *Payload),
	}
}

func (parser *Parser) ParseStream() <-chan *Payload {
	go parser.parsing()
	return parser.ch
}

func (parser *Parser) parsing() {
	// 异常处理
	defer func() {
		if err := recover(); err != nil {
			logger.Error(err, string(debug.Stack()))
		}
	}()
	// parsing
	for {
		line, err := parser.reader.ReadBytes('\n')
		if err != nil {
			parser.ch <- &Payload{Err: err}
			close(parser.ch)
			return // 出现错误，终止
		}
		length := len(line)
		if length <= 2 || line[length-2] != '\r' {
			continue // 忽略空行
		}
		line = bytes.TrimSuffix(line, []byte{'\r', '\n'}) // 去掉末尾的CRLF
		// 根据line[0]进行分发
		switch line[0] {
		case '+':
			// 简单字符串(Simple String)
			err := parser.parseSimpleString(line)
			if err != nil {
				parser.ch <- &Payload{Err: err}
				close(parser.ch)
				return
			}
		case '$':
			// 字符串(Bulk String)
			err := parser.parseBulkString(line)
			if err != nil {
				parser.ch <- &Payload{Err: err}
				close(parser.ch)
				return
			}
		case '*':
			// 数组(Multi Bulk Strings)
			err := parser.parseMultiBulk(line)
			if err != nil {
				parser.ch <- &Payload{Err: err}
				close(parser.ch)
				return
			}
		case ':':
			// 整数(Integer)
			err := parser.paresInteger(line)
			if err != nil {
				parser.ch <- &Payload{Err: err}
				close(parser.ch)
				return
			}
		case '-':
			// 错误信息(Error)
			reply := reply3.MakeErrReply(string(line[1:]))
			parser.ch <- &Payload{Data: reply}
		default:
			// 其他情况
			args := bytes.Split(line, []byte{' '})
			reply := reply3.MakeMultiBulkReply(args)
			parser.ch <- &Payload{Data: reply}
		}
	}
}

func (parser *Parser) paresInteger(line []byte) error {
	value, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		parser.handleError("illegal number '" + string(line[1:]) + "'")
		return nil
	}
	reply := reply3.MakeIntReply(value)
	parser.ch <- &Payload{Data: reply}
	return nil
}

func (parser *Parser) parseSimpleString(line []byte) error {
	status := string(line[1:])
	reply := reply3.MakeStatusReply(status)
	parser.ch <- &Payload{Data: reply}
	//if strings.HasPrefix(status, "FULLRESYNC") {
	//	err := parser.parseRDBBulkString()
	//	return err
	//}
	return nil
}

func (parser *Parser) parseBulkString(header []byte) error {
	size, err := strconv.ParseInt(string(header[1:]), 10, 64) // 解析正文长度
	if err != nil || size < -1 {
		parser.handleError("illegal bulk string header '" + string(header) + "'")
		return nil
	} else if size == -1 {
		reply := reply3.MakeNullBulkReply() // Null Bulk String
		parser.ch <- &Payload{Data: reply}
		return nil
	} else {
		body := make([]byte, size+2) // 正文长度+CRLF的长度
		_, err = io.ReadFull(parser.reader, body)
		if err != nil {
			return err
		}
		args := body[:len(body)-2] // 去掉末尾的CRLF
		reply := reply3.MakeBulkReply(args)
		parser.ch <- &Payload{Data: reply}
		return nil
	}
}

func (parser *Parser) parseMultiBulk(header []byte) error {
	size, err := strconv.ParseInt(string(header[1:]), 10, 64) // 解析数组长度
	if err != nil || size < 0 {
		parser.handleError("illegal multi bulk header '" + string(header[1:]) + "'")
		return nil
	} else if size == 0 {
		reply := reply3.MakeEmptyMultiBulkReply() // Empty Multi Bulk Strings
		parser.ch <- &Payload{Data: reply}
		return nil
	}
	bulks := make([][]byte, 0, size)
	for i := int64(0); i < size; i++ {
		header, err := parser.reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		length := len(header)
		if length < 4 || header[0] != '$' || header[length-2] != '\r' {
			parser.handleError("illegal bulk string header '" + string(header) + "'")
			break
		}
		size, err := strconv.ParseInt(string(header[1:length-2]), 10, 64) // 解析当前bulk string的正文长度
		if err != nil || size < -1 {
			parser.handleError("illegal bulk string length '" + string(header) + "'")
			break
		} else if size == -1 {
			bulks = append(bulks, []byte{}) // null buck string
		} else {
			body := make([]byte, size+2) // 正文长度+CRLF长度
			_, err := io.ReadFull(parser.reader, body)
			if err != nil {
				return err
			}
			bulks = append(bulks, body[:len(body)-2]) // 去掉末尾的CRLF
		}
	}
	reply := reply3.MakeMultiBulkReply(bulks)
	parser.ch <- &Payload{Data: reply}
	return nil
}

// there is no CRLF between RDB and following AOF, therefore it needs to be treated differently
func (parser *Parser) parseRDBBulkString() error {
	header, err := parser.reader.ReadBytes('\n')
	header = bytes.TrimSuffix(header, []byte{'\r', '\n'})
	if len(header) == 0 {
		return errors.New("empty header")
	}
	strLen, err := strconv.ParseInt(string(header[1:]), 10, 64) // 解析字符串长度
	if err != nil || strLen <= 0 {
		return errors.New("illegal bulk header: " + string(header))
	}
	body := make([]byte, strLen)
	_, err = io.ReadFull(parser.reader, body) // 读取字节到body直到body填满
	if err != nil {
		return err
	}
	parser.ch <- &Payload{
		Data: reply3.MakeBulkReply(body[:]),
	}
	return nil
}

func (parser *Parser) handleError(msg string) {
	err := errors.New("RESP error: " + msg)
	parser.ch <- &Payload{Err: err}
}

package redis

import (
	Dict "go-redis/datastruct/dict"
	List "go-redis/datastruct/list"
	_interface "go-redis/interface"
	Reply "go-redis/resp/reply"
	"go-redis/utils/sync"
	"strconv"
)

type Pubsub struct {
	table  Dict.Dict[string, List.List[_interface.Client]]
	locker *sync.Locker
}

func MakePubsub() *Pubsub {
	return &Pubsub{
		table:  Dict.MakeConcurrentDict[string, List.List[_interface.Client]](8),
		locker: sync.MakeLocker(16),
	}
}

func (ps *Pubsub) Subscribe(client _interface.Client, channels []string) _interface.Reply {
	// 上锁
	ps.locker.Locks(channels...)
	defer ps.locker.UnLocks(channels...)
	// subscribe
	for _, channel := range channels {
		client.Subscribe(channel)
		subscribers, ok := ps.table.Get(channel)
		// 当前channel不存在
		if !ok {
			subscribers = List.MakeDLinkedList[_interface.Client]()
			ps.table.Put(channel, subscribers)
		}
		equalFunc := func(target _interface.Client) bool {
			return client == target
		}
		if subscribers.Contains(equalFunc) {
			continue
		}
		subscribers.Add(client)
		reply := Reply.StringToArrayReply("subscribe", channel)
		_, _ = client.Write(reply.ToBytes())
	}
	return Reply.MakeEmptyBulkReply()
}

func (ps *Pubsub) UnSubscribe(client _interface.Client, channels []string) _interface.Reply {
	// 上锁
	ps.locker.Locks(channels...)
	defer ps.locker.UnLocks(channels...)
	//
	if len(channels) == 0 {
		reply := Reply.StringToArrayReply("unsubscribe", "-1", "0")
		_, _ = client.Write(reply.ToBytes())
	}
	// unsubscribe
	for _, channel := range channels {
		client.UnSubscribe(channel)
		subscribers, ok := ps.table.Get(channel)
		// 当前channel不存在
		if !ok {
			continue
		}
		equalFunc := func(target _interface.Client) bool {
			return client == target
		}
		subscribers.RemoveAll(equalFunc)
		if subscribers.Len() == 0 {
			ps.table.Remove(channel) // 无任何订阅者，移除该channel
		}
		reply := Reply.StringToArrayReply("unsubscribe", channel, strconv.Itoa(client.ChannelsCount()))
		_, _ = client.Write(reply.ToBytes())
	}
	return Reply.MakeEmptyBulkReply()
}

func (ps *Pubsub) Publish(client _interface.Client, channel string, message []byte) _interface.Reply {
	// 上锁
	ps.locker.Lock(channel)
	defer ps.locker.UnLock(channel)
	subscribers, ok := ps.table.Get(channel)
	if !ok {
		return Reply.MakeIntReply(0)
	}
	respFunc := func(i int, c _interface.Client) bool {
		reply := Reply.StringToArrayReply("message", channel, string(message))
		_, _ = c.Write(reply.ToBytes())
		return true
	}
	subscribers.ForEach(respFunc)
	return Reply.MakeIntReply(int64(subscribers.Len()))
}

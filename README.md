本项目为使用go语言实现的redis数据库，支持redis的基础功能：

- String、List、Hash、Set、ZSet 的基础功能
- TTL 功能
- publish/subscribe 
- 事务支持：Multi、Exec、Discard、Watch、UnWatch 命令 
- AOF 持久化、 AOF 重写 
- Config 配置：config set、config get


压测
redis-benchmark -h localhost -p 6666 -c 200 -n 10000 -q
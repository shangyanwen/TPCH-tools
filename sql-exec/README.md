## 项目名称
> SQL执行工具


## 运行条件
> Linux、MacOS、windows

## 使用说明

1. 基于Golang实现的sql文件执行工具（无需安装其它依赖），并输出执行耗时报告
2. 源码：main.go
3. 可执行文件：bin/sql

Demo：
```
# 4个线程并发执行/tmp/query.sql文件中查询语句
./bin/sql -uname root -pwd 123456 -host 127.0.0.1 -port 3306 -db test -t 4 -type query -fd /tmp/query.sql
```

运行参数说明：
- -uname：数据库用户名
- -pwd：数据库密码
- -host：数据库IP
- -port：数据库端口
- -db：数据库名称
- -t：线程数（默认1）
- -fd：SQL文件/导入数据目录(绝对路径)
- -type：query/ddl/load（query-执行查询语句; ddl-DDL语句; load-导入数据）
- -fsplit：导入数据字段分隔符，默认：'|'
- -lsplit：导入数据行分隔符，默认：'\n'
  

## 项目名称
> tpch rf测试工具


## 运行条件
> Linux、MacOS、windows

## 使用说明

1. 基于Golang实现tpc-h rf测试工具
2. 源码：main.go
3. 可执行文件：bin/tpch-rf

Demo：
```
# 测试tpch 10G数据的rf1流程
./bin/tpch-rf -uname root -pwd 123456 -host 127.0.0.1 -port 3306 -db test -type rf1 -fd /tmp -sf 10
```

运行参数说明：
- -uname：数据库用户名
- -pwd：数据库密码
- -host：数据库IP
- -port：数据库端口
- -db：数据库名称
- -fd：数据文件目录
- -type：rf1/rf2
- -sf：tpch数据大小（G）
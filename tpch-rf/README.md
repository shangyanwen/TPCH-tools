## 项目名称
> tpch rf测试工具


## 运行条件
> Linux、MacOS、windows

## 使用说明

1. 基于Golang实现tpc-h rf测试工具
2. 源码：main.go
3. 可执行文件：bin/tpch-rf
4. 执行rf1直接用生成的rf文件，如：lineitem.tbl.u1，执行rf2需要将rf文件重命名加上delete，如：delete-lineitem.tbl.u1
5. 生成RF文件的命令：./dbgen -v -U 1     #生成1G数据量的RF，结果文件包含lineitem和orders

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

# dbtool
mongodb tools version 1.1 with backup and sync for 

微信号：ydq580

邮箱：2960428571@qq.com

QQ群：62163057

# 2017-7-30号

个人时间总是有限的，总会有时间没钱，没钱有时间啥的理由，无法做到实时维护代码，所以

提交了所有代码，提供给对有需要的同学自行下载编译生成二进制后直接使用

# 同时，代码可以任意修改，以及提交更好的逻辑块到这里，以后大家一起共享。

# 编译方法:

cd mongorsync-1.1/mongorsync/

go build main/mongorsync.go

同理 mongobackup 一样

手里没机器了,有点儿懒，就把图留了下来

同步数据列子：

./mongosync -H 127.0.0.1 --fport=27017 -h127.0.0.1 --port 27018 --oplog --drop

如果你想删除掉目标库对应的表就加上--drop 

![image](https://github.com/YuanDdQiao/dbtool/blob/master/image/sync.jpeg)

备份列子：

./mongobackup -h127.0.0.1 --port 27017 --gzip --oplog --numParallelcollections=5 --out /backup_test/data/

如果你想备份的快点，那就加大并发数 numParallelcollections ，如果不加--oplog 就和dump 一样的功能。

备份后数据的目录结构和Mongodump一样，还原的时候用Mongo自带的mongorestore 就可以还原。

![image](https://github.com/YuanDdQiao/dbtool/blob/master/image/backup1.jpeg)

![image](https://github.com/YuanDdQiao/dbtool/blob/master/image/backup2.jpeg)

# 2017-8-24
最后的感谢：希望帮忙点个star 呗！（此处是非奸诈的表情）


如果不能编译：可直接下载二进制文件使用

sync:
https://github.com/YuanDdQiao/dbtool/tree/master/binDownload/MongoSyncTools

backup:

https://github.com/YuanDdQiao/dbtool/tree/master/binDownload/MongoBackUp

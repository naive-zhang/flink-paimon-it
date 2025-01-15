
### 生产环境中如何使用Paimon


### Tag和Snapshot之间如何管理

* Tag
  * 每小时一个Tag, 可以用于细粒度的数据增量读取 
  * 保留3个月的增量数据(24*90=2160个Tag)
* Snapshot
  * 以checkpointInterval=5min为例
  * 12个snapshot可以cover住两个tag之间的interval
    * 防止一些意外, minSnapshotRetainNum = 14?
  * 拍脑定3h要36个snapshot, maxSnapShotRetationNum = 48 ?
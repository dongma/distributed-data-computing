# 大数据的三驾马车之HDFS

> `Hadoop`是`Apache`的一个开源分布式计算平台，核心是以`HDFS`分布式文件系统和`MapReduce`分布式计算框架构成，为用户提供了一套底层透明的分布式基础设施。其中`HDFS`提供了海量数据的存储，`MapReduce`提供了对数据的计算。

使用`docker`在本地快速构建`hadoop`服务，镜像名称为`sequenceiq/hadoop-docker:latest`

```bash
> docker run -p 50070:50070 -p 9000:9000 -p 8088:8088 -it sequenceiq/hadoop-docker /etc/bootstrap.sh -bash
```


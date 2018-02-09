# css-elasticsearch
关于Elasticsearch老版本v2.4.6中jar依赖与业务项目jar包的冲突的完美解决，并扩展HTTP、Transport接口

解决一下jar包冲突：
```
1.log4j
2.jackson
3.io.netty以及jboss.netty
4.fastjson
```
## 使用方式：

1. Maven配置
------------
```xml
<dependency>
    <groupId>com.ucloudlink.css</groupId>
    <artifactId>css-elasticsearch</artifactId>
    <version>2.4.6</version>
</dependency>
```

2.业务集成重写common/ElasticsearchSingleton.java即可
-------------------------------------
3.ES Version VS Log4j Vesion
----------------------------

ES version | Log4j version
-----------|-----------
2.4.6 | 1.2.17


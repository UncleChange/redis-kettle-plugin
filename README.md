# redis-kettle-plugin 
本项目各个插件主要为了服务在kettle抽取数据时的增量缓存实现
redis-kettle-plugin 主要实现了kettle的redis数据输入/输出插件，按照kettle插件规则打包放人data-integration\plugins 既可使用
--注释编码GBK
## 插件
1 redis数据源输入
2 redis数据源输出
3 redis缓存重置

使用方法见demo文件夹《Redis缓存重置例子.ktr Redis增量缓存主数据例子.ktr redis增量删除数据获取用法.ktr》

plugins打包方式见demo文件夹elasticsearch-bulk-insert-plugin.zip

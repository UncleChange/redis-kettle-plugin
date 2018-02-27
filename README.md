# redis-kettle-plugin 
本项目各个插件可通过组合使用，在kettle抽取数据时实现增量缓存功能
###
redis-kettle-plugin 主要实现了kettle的redis数据输入/输出插件，按照kettle插件规则打包放人data-integration\plugins 既可使用
###
--注释编码GBK
## 插件包含
1 redis数据源输入
2 redis数据源输出
3 redis缓存重置
## demo
使用方法见demo文件夹《Redis缓存重置例子.ktr Redis增量缓存主数据例子.ktr redis增量删除数据获取用法.ktr》

plugins打包方式见demo文件夹elasticsearch-bulk-insert-plugin.zip
###
插件加载成功可在 转换->核心对象->应用  下见到三个redis新增图标 可以拖动使用

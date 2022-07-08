# 星盘编译原理

+ 星盘使用[ANTLR4](https://github.com/antlr/antlr4)将flow文件编译成ProtoBuf中间文件存放在内存中，ANTLR是一款强大的语法分析器生成工具，可用于读取、处理、执行和翻译结构化的文本或二进制文件。ANTLR可以为自定义的语言生成一个词法分析器，语法分析器，语义分析器并自动创建语法分析树。

![编译原理](.././assets/images/compile-process.png)

+ Protobuffer表示语法树信息，并进行扩展支持compositor、自定义算子builtin等；根据配置文件和flow内容生成编排关系flowGraph。
+ flowGraph的顶点是flow，边支持include,subscribe,schedule,callback关系，include和schedule为flow语法支持 ,subscribe和callback为配置文件支持。

+ [星盘ANTLR4语法定义](./Flow.g4)

+ [星盘中间文件格式](./flow_dsl.proto)

+ **股票舆情案例代码**:[stock.flow](./stock.flow)

+ **股票舆情案例代码编译后ProtoBuf中间文件**:[stock.proto](./stock.proto)

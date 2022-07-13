# 星盘编译原理

## 星盘编译整体流程

![编译原理](.././assets/images/compile-process1.png)

+ 星盘使用[ANTLR4](https://github.com/antlr/antlr4)将flow文件编译成AST(抽象语法树), AST用Protobuf表示，[ProtoBuf定义](../../horoscope-core/src/main/proto/flow_dsl.proto)。

+ Protobuf扩展支持compositor、自定义算子builtin等，Protobuf和FlowConf配置一起生成单个的Flow对象。
+ 所有的Flow对象一起生成一个flowGraph，flowGraph的顶点是Flow对象，边为Flow对象之间的关系，现在支持include,subscribe,schedule,callback关系
+ flowGraph交给FlowExecutor执行，后续支持分布式FlowExecutor

## 星盘关键定义文件
+ [星盘语法定义](../../horoscope-core/src/main/antlr4/com/didichuxing/horoscope/dsl/Flow.g4)

## 案例

+ **股票舆情案例代码**:[stock.flow](../../horoscope-examples/flow/stock.flow)

+ **股票舆情案例代码编译后ProtoBuf中间文件**:[stock.proto](./stock.proto)

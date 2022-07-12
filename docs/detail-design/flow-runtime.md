# 1.星盘系统整体架构

![整体架构](.././assets/images/arch1.png)

## 接入层
+ 负责接收外界事件，可能是kafka、[DDMQ](https://github.com/didi/DDMQ)等异步消息，也可能是http等同步消息
+ 事件源对应一个Source，不同Source之间是隔离的
+ 接入层对消息分区，将不属于自己的消息转发给其他服务器，消息按Source的维度转发
+ 接入层是有状态的，对状态的存取由接入层调用存储层完成

## 计算层
+ 计算层是无状态了，保证高效的完成事件触发的计算任务
+ 计算层通过一套DSL串联各种计算任务，计算任务可以是本地任务，也可以是远程服务
+ FlowExecutor由父子Actor实现，目的是提升并发执行能力

## 存储层
+ 负责保存星盘系统的所有状态
+ mailBox:未完成事件池，已经进入星盘，但是未完成的事件
+ traceContext：每个event执行的上下文
+ schedulerBox：延迟触发或持续监控的消息池
+ clusterInfo：机器服务器信息

## 资源管理
+ FlowExecutor资源的分配，如：并发度等
+ 后期负责星盘分布式集群的资源分配，如：集群的选主、扩容、缩容等


# 2.典型执行流程
![执行流程](.././assets/images/executor1.png)
+ kafkaSource负责消费kafka中数据，并维护kafka位点进行commit
+ EventBuilder负责事件的生成
+ EventRouter负责事件分发
+ EventProcessor负责获取反压并使用actor放入mailbox中
+ 相同traceId必须串行执行
+ 一个trace上，后面的flow能看到前一个flow的修改
+ 使用actor模型，将事件放入mailbox中，后续由Akka异步执行

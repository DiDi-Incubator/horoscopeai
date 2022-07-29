# 星盘拓展
## 服务调用拓展/Compositor
作为模型编排框架，星盘首要的职责就是串联推理服务、数仓服务和外部系统。
当一次事件触发后，通常需要调用外部来对数据做组织和封装，以便后续作为判断的逻辑或输出的结果。
我们将这些外部调用都看成是对数据做加工和组织(composite)的过程，所以在星盘中称之为compositor。

+ ### Compositor的表述形式
compositor在flow编制文件中声明的语法如下：

```
* CompositorName
\``` factoryName
     code...
\```
```
RestfulCompositor的demo示例如下：
```
(截自horoscope-examples/flow/stock.flow)
# /demo/stock

//Compositor的注册示例,start...
* GetStock
\``` restful
get https://money.finance.sina.com.cn/quotes_service/api/json_v2.php/CN_MarketData.getKLineData?symbol=${stock}&scale=${scale}&ma=no&datalen=${count}
\```
//Compositor的注册示例,end...

***
stock_data <- @stock_data
stock_id <- stock_data["stock_id"]
scale <- stock_data["scale"]
count <- stock_data["count"]

//以下为Compositor的调用示例
stock_result <- GetStock(stock=stock_id, scale=scale, count=count)
***
```

+ ### Compositor的原理
星盘拿到上述信息之后，会利用CompositorFactory接口生成Compositor  
具体过程如下：  
1、用factoryName字段(demo中的"restful")来寻找CompositorFactory  
2、将code文本(demo中的"get"以及"https...")传入factory来创建新的Compositor  
3、随后在事件触发时执行该Compositor实例的composite方法

+ ### 拓展方法及示例
想要扩展星盘中支持的外部服务类型，用户只要实现Compositor及其相应的CompositorFactory就可以。
```java
public interface Compositor {
  Future[Value]  composite(ValueDict valueDict);
}
 
public interface CompositorFactory extends Serializable {
  String name();
 
  Compositor create(String code);
}
```

以项目中的RestfulCompositor为例(伪代码)：
```scala
package com.didichuxing.horoscope.service.compositor

class RestfulCompositor extends Compositor{
  val String url
  override def composite(args: ValueDict): Future[Value] = {
    try {
      restfulConfig.getHttpMethod match {
      case "get" =>
        //get请求，只需要处理url中的参数替换符
        doGet(CompositorUtil.updateUrlByArguments(url, args))
      case "post" =>
        //post请求，除url中的参数替换符外，还需要处理requestBody
        doPost(CompositorUtil.updateUrlByArguments(url, args))(args.visit(restfulConfig.getPostBodyKey
      case m@_ =>
        Future.failed(CompositorException(s"Unsupported restful method: $m"))
      }
    } catch {
      case e: Throwable =>
        Future.failed(CompositorException(e.getMessage, Option(e)))
    }
  }
}

class RestfulCompositorFactory extends CompositorFactory {
  //Compositor的name定义，对应Compositor语法中的factoryName属性，该字段用于在编译解析时寻找该类型Compositor的Factory
  override def name(): String = "restful"

  /**
   * Compositor实例生成逻辑
   * @param code: method[get/post] http://url/${link}\n${post_body}
   *              ${variable}: variable will be filled at runtime
   * @return
   */
  override def create(code: String)(resource: String => Array[Byte]): RestfulCompositor = {
    //解析.flow文件中Compositor定义时的"code"部分
    val flowConfig = CompositorUtil.parseConfigFromFlow(code)
    
    //封装RestfulCompostor需要的配置参数，例如http请求类型、post请求的RequestBody等
    val restfulConfig = new RestfulCompositorConfig(config, flowConfig)
     
    //创建并返回属于这个.flow文件的单例Compositor实例
    new RestfulCompositor(restfulConfig)
  }
}
```
完成Compositor和CompositorFactory的开发后，需要将Factory注册，代码如下：
```scala
GitFlowStore.newBuilder()
.withCompositorFactory("restful", new RestfulCompositorFactory(config))
```

PS:针对任何一个FlowDSL(.flow)配置文件，其中的一个Compositor声明在一个进程中只会生成一个对象，可以认为是单例。
最后特别提醒大家，任何composite行为在星盘内部都是异步处理的，如果外部系统的Client/SDK不提供异步实现，CompositeFactory需要自己通过线程池来满足异步要求。

## 复杂逻辑拓展/UDF&BuiltIn
+ ### UDF注册
通常业务逻辑复杂度很高，不能指望FlowDSL能够满足全部需求，因此星盘允许定义UDF，由用户自由编写一段逻辑或方法并注册后，就可以在FlowDSL中使用它。
UDF和FlowDSL配置文件不同，添加和更新UDF不支持热更新，需要重新编译并发布服务。
UDF有下述两种调用形式(function式、method式)。星盘UDF只支持位置参数
```
// function语法如下：面向过程风格
func(arg1, arg2, ...)
//function在FlowDSL中的使用方式例如：split("ab1cd", "1")，结果：["ab","cd"]
 
// method语法如下：面向对象风格
obj.method(arg1, arg2, ...)
//method在FLowDSL中的使用方式例如：[1, 4, 5].length()，结果：3
```
上述function和method为UDF的两种调用和使用形式，星盘中支持两种语言来编写UDF，分别是基于Scala和基于Python。

+ ### 基于Scala的UDF拓展方式
基于Scala的拓展需要用户在星盘框架代码中编写一个方法，并注册到DefaultBuiltIn后，再启动星盘项目就完成注册并且可以使用了，示例如下：
```scala
//基于Scala的UDF拓展
object DefaultBuiltIn {
  val defaultBuiltin: SimpleBuiltIn = new SimpleBuiltIn.Builder()
    .addMethod("length")(length)  //注册method
    .addFunction("split")(split _)  //注册function
    .build()

//method式
  def length(value: Value)(): Int = {
    //类型敏感的场景下，需要注意入参的类型辨别及校验
    value match {
      case doc: Document => doc.children.size
      case text: Text => text.length
      case _ => throw new IllegalArgumentException(s"type ${value.valueType} does not have length method")
    }
  }

//function式
  def split(s: String, regex: String): Array[String] = {
    s.split(regex)
  }
}
```
+ ### 基于Python的UDF注册
基于Python的拓展需要用户在/flow/functions目录中新增一个Python文件，直接定义方法并编写逻辑(目前仅支持Jyhton基础包)，再重启星盘项目后，即可使用，示例如下：
```scala
##定义一个方法并编写逻辑，保存在/functions目录下
##本UDF引自horoscope-examples这个moudule下/flow/functions/sort_by_key.py
def sort_by_key(files):
  def sort_key(s):
    if s:
      try:
        arrays = s.split("-")
        c = arrays[0][4:] + arrays[1] + arrays[2] + arrays[3][0:2]
      except:
        c = -1
      return int(c)

  dic = {}
  for item in files:
    dic[item]= sort_key(item)

  res = []
  for (key, value) in sorted(dic.items(), key=lambda k: k[0]):
    res.append(key)
  return res
    
##该方法在FlowDSL中的的使用示例：sort_files <- sort_by_key(files)
##详见/flow/analysis.flow
```

## 数据源的拓展/Source
作为一个编排框架，入口数据可以认为是一切流程的起点，在星盘中，数据源(Source)即为数据的入口。
本期开源的版本中，支持了两种数据源用来接收数据流入，HttpSource(http形式)和KafkaSource(Kafka形式)。
用户可以根据需要自行拓展如RocketMQSource，或读本地CSV文件LocalCSVSource等......
+ ###拓展方法及示例：
想要对数据源做拓展，只需要实现Source和与之对应的SourceFactory即可：
```scala
trait Source {
  def start(eventBus: EventBus): Unit

  def stop(): Unit

  def eventBus(): EventBus
}

trait SourceFactory extends Serializable {
  def newSource(config: Config): Source
}
```
具体实现可以借鉴项目中com.didichuxing.horoscope.service.source.KafkaSource。
想要使用拓展的数据源，是通过FlowManager的startSource()方法进行注册的。
对于开发者来说，需要将factory名和source名加入配置文件(horoscope.sources[]目录下)，星盘启动的时候会自动扫描并注册数据源，目前开源版本中关于数据源的配置详情如下：
```properties
horoscope {
  sources = [
    {
      #使用哪种类型的消息接入源，如：kafka，json数据格式
      factory-name = "batchJsonKafka"
      #消息源名称，必须唯一，尽量简短
      source-name = "s1"
      #消息源对应的处理flow(flow配置)
      flow-name = "/root/flow1"
      #消息源接入参数，这里是采用的kafka接入
      parameter {
        kafka = {
          #kafka服务配置，因为涉及ip等敏感信息，这里做了隐藏处理
          #服务地址
          servers = ?
          cluster-id = ?
          app-id = ?
          password = ?
          #topic名，多个用,隔开
          topic = ?
          #group名
          group = ?
          #每次最多拉取消息条数
          max = ?
          #同时拉取的并发线程数
          concurrency = ?
        }
        backpress {
          #该消息源的反压配置100表示，每台机器最多同时有100个消息正在运行
          permits = 100
          #反压等待超时时间
          timeout = 10
        }
      }
    }
    {
      factory-name = "httpSource"
      source-name = "debug"
      parameter = {
        backpress {
          permits = 100
          timeout = 10
        }
      }
    }
  ]
}
```

## I/O交互拓展/Store
### 各类Store简介及接口
+ ### FileStore:用于读写FlowDsl文件
拓展需要实现FileStore接口，可以参考开源版本中的默认实现com.didichuxing.horoscope.service.storage.LocalFileStore
```scala
trait FileStore {

  def getFile(path: String): File

  def listFiles(url: String): (String, Seq[File])

  def updateFile(path: String, content: String): Boolean

  def deleteFile(path: String): Boolean

  def createFile(path: String, isDirectory: Boolean): Boolean

  def copyFile(path: String): Boolean

  def renameFile(path: String, name: String): Boolean

  def api: Route = _.reject()
}
```

+ ### ConfigStore:用于读取配置，并非项目配置文件，而是星盘高级功能的配置，埋点、订阅、实验、回调的相关配置
拓展需要实现ConfigStore接口，可以参考开源版本中的默认实现com.didichuxing.horoscope.service.storage.LocalConfigStore
```scala
trait ConfigStore {

  def getConf(name: String, confType: String): Config

  def getConfList(confType: String): List[Config]

  def registerListener(listener: ConfigChangeListener): Unit

  def registerChecker(checker: ConfigChecker): Unit

  def api: Route = _.reject()
}
```

+ ### FlowStore:星盘I/O交互的核心枢纽，上文中文提到的FileStore、ConfigStore都集成在FlowStore中，其他包括Experiment(实验相关)、BuiltIn(复杂逻辑拓展/UDF)也都集成在FlowStore中
拓展需要实现FlowStore接口，可以参考开源版本中的默认实现com.didichuxing.horoscope.service.storage.GitFlowStore
```scala
trait FlowStore {
  def getFlow(name: String): Flow = throw new NotImplementedError()

  def getController(name: String): Seq[ExperimentController] = throw new NotImplementedError()

  def getFlowGraph: FlowGraph = throw new NotImplementedError()

  def getBuiltIn: BuiltIn

  def api: Route = _.reject()
}
```

+ ### TraceStore:用于存储星盘中的"事件"
将来在分布式版本中，可以基于traceStore实现"事件"的分发，有重要的意义。  
拓展需要实现TraceStore接口，可以参考开源版本中的默认实现com.didichuxing.horoscope.service.storage.DefaultTraceStore
```scala
trait TraceStore {
  /**
    * Add a new event to store, try to acquire and return token when needed
    */
  def addEvent(source: String, event: FlowEvent.Builder): FlowEventOrBuilder

  /**
   * Batch add new events to store
   */
  def addEvents(source: String, events: List[FlowEvent.Builder]): List[FlowEventOrBuilder]

  /**
    * Extract information from instance, and must perform three steps in transaction:
    * 1. save all $variable updates to context
    * 2. if next event exists, put it to mailbox, with source set to null
    * 3. remove event from mailbox
    *
    * Besides, store can save instance to history for analyzing purpose
  */
  def commitEvent(source: String, instance: FlowInstance.Builder): FlowInstanceOrBuilder

  /**
    * Get all pending events from source
    */
  def getEventsBySource(source: String, beginSlot: Int, endSlot: Int): Iterable[FlowEventOrBuilder]

  /**
    * Get most recent snapshot of trace context
    */
  def getContext(trace: String, keys: Array[String]): Future[Map[String, TraceVariableOrBuilder]]

  /**
   * Poll scheduler event
   */
  def pollSchedulerEvents(source: String, slot: Int, timestamp: Long, limit: Int): Iterable[FlowEvent]

  /**
   * Commit success process scheduler event
   */
  def commitSchedulerEvents(source: String, slot: Int, events: List[FlowEvent]): Long

  def api: Route = _.reject()
}
```

想要拓展的开发者实现以上各类Store后， 注册到星盘的启动管理器FlowManager中即可生效。示例如下：
```scala
var flowManager = Horoscope.newLocalService()
  .withFileStore(fileStore)
  .withConfigStore(configStore)
  .withFlowStore(flowStore)
  .withTraceStore(traceStore)
  ... //这里将其他配置的类省略
  .build()

//启动主服务
info("horoscope begin start service")
flowManager.startService()
//启动外部source
info("horoscope begin start sources")
flowManager.startSources()
info("horoscope init complete")
```


PS：以FileStore为例，当前开源版本的Demo使用的是LocalFileStore，是借助本地文件来实现flow文件的读写的。GitFIleStore是基于版本控制这个需求下产生的衍生类。用户可以借助一些分布式工具进行拓展比如ZookeeperFileStore，可以实现交互式开发和多人协作开发。
星盘中的store均为接口形式，目前的实现仅为本地版本，后续会继续开源分布式版本。敬请期待...

## 实验拓展/Experiment
星盘实验是期望能够对模型召回进行评估的设计，只需要实现ExperimentController和ExperimentControllerFactory即可。
```scala
package com.didichuxing.horoscope.core

trait ExperimentControllerFactory {
  def name: String

  def create(config: Config)(builtIn: BuiltIn): ExperimentController
}

// The experiment controller is one-to-one with the experiment conf.
trait ExperimentController {

  def query(args: ValueDict): Option[ExperimentPlan]

  def dependency: Map[String, Expression]

  def priority: Int
}
```
当前星盘开源版本提供了AB实验的代码实现，代码见com.didichuxing.horoscope.runtime.experiment.ABTestController

## 控制台拓展(前端)/Api
当前星盘开源版本的Example仅支持本地编写FlowDSL(.flow)文件，后续会开源星盘控制台(可交互式页面)版本，用户可以直接在页面上查看和开发FlowDSL(.flow)文件、可以在页面上查看执行过的事件、可以用真实数据调试等等...  
当前版本上述功能的接口已经暴露(见GitFlowStore.api)，用户可根据自己的想法设计前端页面交互形式，
星盘后续会开源一个前端控制台，敬请期待......
# 开发指南
## FlowDSL
+ 数据处理
    + 数据结构  
    流转过程中的数据结构和JSON类似，可以是基本类型（整数/浮点数/字符串/布尔类型等）、词典({}/Dict)和列表([]/List)的任意组合。在星盘运行时会表示成Value类型的对象，数据结构内部实现详细文档参见[星盘数据结构](./docs/programming-guides/data-structure.md)。
        ```
        1                         // 整数
        0.9                       // 浮点数
        true false                // 布尔值
        "hello world"             // 字符串
        {"a": 1, "b": 2}          // 词典
        [1, 2, 3, 4]              // 列表
        [{"a": [1, 2, 3, 4]}]     // 复合类型
        ```
    + 服务调用/Composite  
    作为流程编排框架，星盘首要的职责就是串联特征服务、模型服务和外部系统。我们将所有的外部调用看成是对数据做加工和组织的过程，所以在星盘中称之为compositor，认为其职责是对数据做composite。
        + Compositor声明
            ```
            1 * CamelName
            2 ``` factory
            3 code
            4 ```
            ```
            在编制过程中可以使用CamelName来引用这个compositor，方式类似函数调用。Composite的每个参数都必须有名字，不支持位置参数。具体的调用调用形式参考[流程编制/编制语句]章节。
        + 默认支持Compositor
            + RestfulCompositor，通过HTTP的Get/Post方法和外部服务交互，code是某种Template语法用于生成url和request body
            + KafkaProducerCompositor，通过kafka发送消息  
        + Compositor扩展  
        想要扩展星盘中支持的外部服务类型，用户只要实现CompositorFactory就可以。具体的扩展方式参加horoscope-core下的RestfulCompositor即可。稍后，在服务重启之前，需要将新的CompositorFactory集成到FlowStore中。  
            ```
            GitFlowStore.newBuilder()
             .withCompositorFactory("restful", new RestfulCompositorFactory(config))
            ```

    + ETL过程 / 表达式  
    在进行复杂编制时为了串联各种compositor，需要引入大量的ETL逻辑。为了方便支持ETL，星盘在JSONPath基础上扩展了表达式的语法。
        + 特殊标识符
            + $: $前缀的名字用来引用全局变量。(因为我们采用Actor Model，所以允许同一trace上的事件处理共享上下文, trace的含义可查看详细设计/运行时原理章节)
            + @: @前缀的名字用来引用Flow的参数
        + 常量
            + 符合JSON语法的文本
                ```
                text <- "hello world!"
                number <- 1.2
                boolean <- true
                list <- [1, 2]
                dict <- {
                    "a": 1,
                    "b": 2
                }
                ```
            + 文档定义中支持表达式嵌入
                ```
                x <- [1 + 2, 3 + 5]
                y <- {
                    "text": "hello" + "world"
                }
                ```
        + 基本运算
            + 数值运算
                + 支持+ - * / %五种运算符，乘法类操作结合性优先于加法类操作，同一优先级下从左到右结合
                + 所有的数值在星盘内部都用BigNumber来表示，这一点上类似JavaScript. 所以(1 / 2)的结果为0.5而不是0
                + 在对数据进行内部序列化的时候，浮点部分为空的数字会按照64位整数序列化，不为空的数字会按照双精度浮点数序列化
            + 逻辑运算
                + not/and/or. 注意，这三种操作不能写作!/&&/||，因为星盘流程编制更需要符号，所以这些布尔运算用keyword代替
                + in和not in. 判断元素是否在文档中. 对于词典，左值需为文本，检测该文本是否出现在右值的indice中. 对于列表，会检测右值代表的集合中是否有child和左值相等
                + \>、>=、<、<=. 这些操作只能在数值和文本上进行，其中文本比较按照字典序. null值可参与比较操作，永远返回false，特别强调形如null <= null也会返回false
                + ==、!=. 这两个操作可以对任何对象进行，对于文档会递归判断其children. null == null会返回true
                + 上述逻辑运算优先级递减，同一优先级下从左到右结合
                + 所有非布尔值都可以转化为布尔值，规则类似Python 2的设定，null、数值0、空文档、空字符串会转化为false，其他情形转化为true
            + 文本运算
                + text + text，对字符串做拼接
                + text + number或number + text，将number转化为字符串然后做拼接
                + text * number，number需为整数，将text重复number次
    + 用户自定义函数 / Builtin  
    当业务逻辑复杂度过高时，上述表达式通常不能满足全部需求。因此，星盘允许定义UDF。目前UDF支持以Scala和Python两种语言来实现，对于python udf，星盘使用Jython将python代码编译成Java字节码来执行。
        + UDF调用形式
            ```
            // function like
            func(arg1, arg2, arg3, ...)

            // method like
            obj.method(arg1, arg2, arg3, ...)
            ```
        + 默认实现Builtin  
        星盘默认提供了数值运算、字符串处理、集合/列表/字典操作等UDF，具体见[星盘默认提供的Builtin](./docs/programming-guides/builtin.md)。
        + UDF扩展
        用户可以根据需要扩展UDF，不过UDF新增或者修改不支持热更新，需要重新编译并发布服务。
            + Scala UDF 
                + 注册UDF：先编写UDF的逻辑，然后通过addFunction或者addMethod注册至BuiltIn类中。具体可参见horoscope-examples中的ExampleBuiltIn。
                + 集成BuiltIn：如果是新增的BuiltIn扩展类，在服务启动前需要将BuiltIn扩展类集成到FlowStore中。具体可参见horoscope-examples中的DemoLocalService。
                    ```
                    // 注册read_file_content udf
                    implicit val exampleBuiltin: BuiltIn = new SimpleBuiltIn.Builder()
                            .addFunction("read_file_content")(read_file_content _) 
                   
                    // exampleBuilin集成至FlowStore
                    GitFlowStore.newBuilder().withBuilin(ExampleBuiltIn.exampleBuiltin)
                    ``` 
            + Python UDF
                + Python UDF免注册，具体的扩展步骤是先编写python函数，然后包含python函数的.py文件放到flow/functions目录下即可。具体示例可参见horoscope-examples package的flow/functions。

## 流程编制
星盘编制的主要设计思路是"代码化的流程图"。整个编制文件看起来像是程序代码，但仍要当成流程图来理解。流程中各个节点的执行顺序可以是串行、并行或者乱序，编制时不应做任何假设。整个执行过程从结果倒推，下游节点在需要时触发上游节点执行。  

**编制语句**:  
+ Assign: 保存中间结果的节点
    ```text
    variable <- expression  // 基本形式
    variable <~ expression  // 延迟执行，variable有需要时才会被计算
    $variable <- expression  // 定义一个名为variable的变量，同时将这个变量存入trace上下文，对后续处理过程可见
    variable <- try-expression !! fallback-expression  // 先尝试运算try-expression，如果运算出错将variable设置为fallback-expression的值. 该语法也常被用来指定默认参数
    ```
+ Composite: 生成服务调用节点
    ```text
    result <- Composite(x=expression, y=expression)  // 基本形式
    result <~ Composite()  // 延迟执行，result有需要时才会产生外部调用
    $result <- Composite()  // 定义一个名为result的变量，同时将这个变量存入trace上下文，对后续处理过程可见
     
    result <- Composite(x)  // 参数的省略形式，要求前文中已经定义了名为x的变量，且Composite的参数名也是x
    DoSomething()  // 执行结果的省略形式，会生成一个名为do_something的变量
    result <- [Composite(x=_.x) <- xs]  // 批式调用形式，要求xs为文档类型，针对每个child调用外部服务，返回值中indices保持不变，child替换为服务调用结果
    ```
+ Branch: 类似C/Java里的Switch-Case语句
    ```
    <> {
        ? is_a = predicate  // predicate是逻辑表达式。会生成一个名为is_a的延迟变量，如果运算结果为true，对应block中的语句会生效（在<~的情况下不会立即执行，而是按需触发）
        => {
            ...
        }
     
        ? is_b  // 省略形式，要求上文中存在名为is_b的变量
        => {
           ...
        }
    
        ? is_c  // 合并形式，如果is_c/is_d两个分支会执行同样的行动，可以利用这种形式简化书写。相比于写成is_c and is_d的形式，主要是生成的日志有所区别
        | is_d
        => {
           ...
        }
    }
    ```
    
## 流程编排
总的来说，星盘流程编排有Include、Subscribe、Schedule、Callback四种形式，其中Include和Schedule是FlowDSl Native实现的，Subscribe、Callback是通过配置文件实现的。
+ Include
include是将其他flow引入到当前flow中，也可以理解为当前flow触发另一个flow的执行。其语法需要指定作用域(scope)，被调用flow名和调用参数。include可以获得调用返回结果，通过stock->stock_result这样的语法，我们可以在后续的编制过程中访问这个include返回结果，注意scope名不能和变量名重复。
```
  <stock> => /demo/stock?stock_data=stock_data  // 指定scope，被调用flow和调用参数
  result <- stock->stock_result  //通过scope获取被调用flow中返回的结果
  ```
+ Subscribe
subscribe是从一个flow中订阅流量，被订阅的flow不感知被订阅，不支持返回值。通过订阅配置来实现订阅，配置中可以配置流量比例，准入流量条件等。
```
  {
  	"name": "stock-subscribe-test",  //订阅配置名称
  	"publisher": "/demo/main",  //发布者flow名
  	"subscriber": "/demo/stock",  //订阅者flow名
  	"args": [{
  		"name": "stock_data",  //变量名
  		"expression": "stock_data"  //变量表达式
  	}],
  	"bucket": [0, 100],  //流量配置
  	"condition": "true",  //流量准入条件
  	"enabled": true  //配置开关
  }
  ```
+ Schedule
schedule是延时调度其他flow，语法和include相似，主要区别是要在scope后面带上+号来代表创建新事件，并且不能用scope -> name来访问结果

```
<schedule> + "10 minutes" => /v2/schedule?x=1&y=2  //延迟执行
```

+ Callback
callback指调用外部服务并接收反馈的机制。调用外部服务后，等待外部服务的反馈来继续接下来的流程运行。可以理解为受外部服务控制的延时调度方式。
星盘中的callback与JavaScript中callback的概念略有不同，JS中的回调函数主要用于异步编程提高性能，而星盘中的回调是为了将流程关联，靠外部反馈信号来触发流程的运算。
例如，流程A中触发图像采集过程等待图像回收，图像收回时，使用callback机制将流程A和回收流程(流程B)关联起来，使得流程B的被触发。
通过回调配置来实现回调机制，回调配置中需要主要指定以下关键信息：
    + 回调token
        回调机制的唯一标识，用于关联回调过程返回结果
    + 回调注册flow、回调flow和超时flow
        用户在回调注册flow处生成token，传入变量中；在回调flow中回收反馈结果，解析token，关联回调前的流程；如果回调超时无反馈，则会触发超时flow执行
    + 超时时间
        当回调超过该时间，触发超时flow执行
    + 上下文中的参数
        即回调时传出的变量

```
{
"name": "info_feedback",  //回调配置名
"timeout": "10 minute",  //回调超时时间
"token": "token_id",  //token变量名
"flow": {
	"callback": "/demo/info-feedback",  //回调返回触发的flow
	"register": "/demo/register-info-callback",  //注册回调的flow
	"timeout": "/demo/info-feedback-timeout"  //超时会触发的flow
	},
"args": [{
	"expression": "input",  //参数表达式
	"name": "callback_info"  //参数名称
	}],
}
```

## 流程埋点
+ 设计初衷  
基于星盘提供的编排能力，复杂业务流程的串联会变得非常方便。但相对地，要想对业务流程进行分析就没有那么容易，尤其是多流程的关联分析。一个常规的方法是，将每个流程的日志分别收集、存储、挂载到表上，在使用时再按照分析需求进行多表的关联。这样的方式虽然可以满足需求，但是离线多表关联效率十分低下。针对这种困境，星盘特地设计了配置化的、跨多流程的主题化埋点方案。
+ 使用介绍
    + 一个埋点主题必须指定一个中心flow，具体的埋点字段可以来自中心flow或中心flow的前、后向flow
        + 中心flow： 主要分析的目标flow
        + 前向flow：与中心flow位于同一flow graph，但先于中心flow调用的flow
        + 后向flow：与中心flow位于同一flow graph，但在中心flow后被调用的flow
        + 示例：比如在我们的场景中，要分析模型预测的置信度分区，则将模型预测flow设置成中心flow，特征处理flow则是前向flow，模型结果后处理flow是后向flow
    + 埋点字段类别：
        + tag： 对flow的调用情况进行埋点，当该flow被调用，则取值为true，反之为false
        + choice：对flow执行的choice结果进行埋点，取值为字符串列表，每个元素是该flow执行时命中的choice名称
        + variable：对flow的中间变量进行埋点，可以直接指定flow原始变量，也可以支持表达式运算
    + 埋点遵循原则
        + 就近埋点: 当一个topic里的flow被执行多次时， 仅记录与中心flow最近的触发
        + 中心flow未调用不记录: 在实际执行时， 如果中心flow并未调用则不进行日志记录； 当中心flow被调用而其他flow未调用时，会在日志中显示地标识flow_tag = false
        + 回调超时: 如果需要记录的字段依赖callback flow，但是一直没有回调，则在超时到达时进行日志结算
+ 使用示例  
以horoscope-examples的股票分析demo为例，介绍主题埋点如何使用
    + 编写埋点配置  
    在这里我们设置了一个叫做stock_analysis的主题埋点，埋点字段涵盖/demo/pipeline、/demo/analysis、/demo/stock三个flow。
    ```
        logs = [
          {
              name = "stock_analysis"          // 埋点主题名称
              enabled = true                   // 埋点配置是否启用
              flow = "/demo/pipeline"          // 中心flow
              fields = [                       // 具体的埋点字段
                  {
                      name = "pipeline_tag"
                      type = "tag"
                      meta = {
                          flow = "/demo/pipeline"
                          forward = false
                      }
                  },
                  {
                      name = "pipeline_choice"
                      type = "choice"
                      meta = {
                          flow = "/demo/pipeline"
                          forward = false
                      }
                  },
                  {
                      name = "stock_id"
                      type = "variable"
                      meta = {
                          flow = "/demo/pipeline"
                          forward = false
                          expression = "stock_data.stock_id"
                      }
                  },
                  {
                      name = "analysis_tag"
                      type = "tag"
                      meta = {
                          flow = "/demo/analysis"
                          forward = false
                      }
                  },
                  {
                      name = "analysis_senti_result"
                      type = "variable"
                      meta = {
                          flow = "/demo/analysis"
                          forward = false
                          expression = "senti_result"
                      }
                  },
                  {
                      name = "stock_tag"
                      type = "tag"
                      meta = {
                          flow = "/demo/stock"
                          forward = false
                      }
                  },
                  {
                      name = "stock_result_size"
                      type = "variable"
                      meta = {
                          flow = "/demo/stock"
                          forward = false
                          expression = "stock_result.length()"
                      }
                  }
              ]
          }
        ]
    ```  
    + 查看埋点结果  
    埋点配置编写完成之后，添加至conf/log.conf文件中，重启服务即可生效。根据docs/examples/demo.md文档，步骤4触发pipeline flow执行，然后在本地日志文件logs/publish.log中查看stock_analysis的埋点结果数据：
    ```
        {
            "field":{
                "analysis_senti_result":[1,0,1,1,1,1,1,0,1,0,0,1,0,1,0,1,1,1,0,1],
                "analysis_tag":true,
                "pipeline_choice":[
                    "is_valid"
                ],
                "pipeline_stock_id":"sz000001",
                "pipeline_tag":true,
                "stock_result_size":6,
                "stock_tag":true
            },
            "log_id":"6293544b6c83811b7851a3f6",
            "log_timestamp":1653822539678,
            "scope":["main"],
            "topic_name":"stock_analysis",
            "trace_id":"5d7a064483d134989206c7a74a4d23ff"
        }    
    ```
   + 埋点数据发送至Kafka  
   星盘还支持直接将埋点结果数据发送至Kafka队列，只需要在application.conf中增加相关配置：
   ```
        topic-logger {
          kafka {
            topic = ""               // kafka 队列名称
            bootstrap.servers = ""   // kafka servers
            ***                      // kafka其他配置项
          }
        }
   ``` 
     
## 实验分析
星盘提供一系列实验分析工具，主要用于模型/策略对比
+ **A/B实验**
星盘流程中的策略或算法(例如demo中的NLP模型)，在生产过程中经常会有策略迭代、模型升级等场景需要进行A/B实验。用户可以创建2组或多组，配置流量，并收集实验数据并对比试验效果。
星盘实验分析模块具有以下功能
    + 自定义灵活多层分流，确保实验数据的有效性
    + 记录实验数据，实时展示实验报告
    + 自动生成包含转化率、等特定指标的实验报告
+ **其他实验探索**
星盘在模型/策略自动优化做了一定探索，例如模型超参数调优（Hyper Parameter Optimization），主动学习（Active Learning）等，将在后续呈现
    + Hyper Parameter Optimization
    星盘可扩展外部超参数调优服务，对策略/模型进行自动化调优，以获得更好的模型表现
    + Active Learning
    同样，星盘可扩展主动学习服务，进行主动学习采样，使模型快速收敛
    + 召回分析
    新旧模型对比时，可使用同一组数据比较两策略的召回差异，可以得出两模型召回差异，分析其特性，有助于模型迭代优化
    + 模型可解释性
    对于漏招样本，星盘提供了模型可解释性工具，例如LIME
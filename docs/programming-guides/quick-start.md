本教程将帮助你快速搭建一个星盘本地服务

# 快速搭建服务
## 本地环境
+ maven 3.3.3版本以上
+ scala 2.11.8
+ protobuf 3.6.0
+ flatbuffers 1.12.0

## 服务编译启动
```shell
mvn clean package -DskipTests
cd horoscope-examples/dist
./bin/service.sh start
```

## 请求服务
触发服务中quickstart.flow执行
```shell
curl --location --request POST 'http://localhost:8062/api/schedule/info/_/now/demo/quickstart' \
--header 'Content-Type: application/json' \
--data-raw '{
    "user_name": "Jane",
    "text": "Hello word"
}'
```

如果返回类似如下json数据, 服务运行成功 

```json
{
    "event": {
        ...
        "flow_name": "/demo/quickstart"
    },
    "procedure": [
        {
           ...
            "assign": {
                ...
            },
            "choice": [
                "is_valid"
            ]
        }
    ]
}
```


# Quickstart Demo简介
## 业务场景
检测用户发布的twitter内容是否合法. 这里检测字符数是否小于150
用户发布的twitter内容包括两个字段user_name和text, 例如:
```json
{
    "user_name": "Jane",
    "text": "Hello world"
}
```

## Flow定义
```text
# /demo/quickstart // 这是flow名称, 可以是任意格式, 但星盘服务一般要求为类似文件的url格式
***
  user_name <- @.user_name // @表示输入参数
  text_length <- @.text.length()
  <> { // 两个括号是if语句, 由于FlowDSL流程图语言, 这里用<>表达分支
    ? is_valid = text_length < 150 // 问号后面的is_valid是这个分支的tag, 用于埋点, 在星盘的概念中, 叫做choice
    => {
    text_is_valid <- true
    }
    
    ? is_not_valid = text_length >= 150
    => {
    text_is_valid <- false
    }    
  }
***
```

## 编写服务入口程序
星盘服务接口有良好的扩展性, 可以自定义config, flowStore, builtin, compositor等组件
```scala
class DemoLocalService extends Logging {

  var flowManager: FlowManager = _
  val config: Config = ConfigFactory.load(Thread.currentThread().getContextClassLoader, "application")

  def run(): Unit = {
    SystemLog.create()
    info("local service init")
    val flowStore = GitFlowStore.newBuilder()
      .withFileStore(new LocalFileStore(config)) // FileStore用于管理flow目录下的文件, 例如flow, python文件
      .withConfigStore(new LocalConfigStore) // ConfigStore放置埋点, 订阅等编排配置
      .build()
    flowManager = Horoscope.newLocalService()
      .withConfig(config)
      .withBuiltIn(flowStore.getBuiltIn)
      .withFlowStore(flowStore)
      .withSourceFactory("httpSource", Sources.http()) // 泛源定义, 这里默认集成了http source
      .build()
    info("start service")
    flowManager.startService()
    info("start sources")
    flowManager.startSources()
    info("service already started")
  }
}
```

## 服务配置
星盘服务配置采用[typesafe config](https://github.com/lightbend/config)的风格
```config
horoscope {
  sources = [
    {
      factory-name = "httpSource"
      source-name = "info"
    }
  ]// 配置泛源, 泛源名称为info, 在服务请求时需要带上该名称

  storage = {
    file-store {
      type = ["py", "flow"]
      local-root-path = ${?DEFAULT_FLOW_BASE_DIR}
    }
  }

  flow-executor {
    detailed-log.enabled = true // flow是否全埋点, 如果埋点信息太多, 可以关闭
    topic-log.enabled = true // 主题埋点是否打开
  }
}
```

## 服务目录组织
```config
-- bin/service.sh 服务启动脚本
-- conf/ 服务配置
-- flow/ 服务运行的flow文件
-- src
----- assembly/assembly.xml 服务编译时, 会将文件打包到dist目录
----- main/../DemoLocalService 服务主入口
```

## 服务运行时日志
日志在logs目录下
  + app.log 服务运行时日志, 日志中的trace_id是用于线上日志监控追踪, 可以暂时忽略
  + public.log 服务执行的flow埋点日志, 由OdsLogger生成, 默认打印到本地, 如果分布式环境中可以发送到kafka
  + gc.log 系统gc信息
  
## 服务请求参数说明
在本Demo中, 触发flow运行的命令如下:
```shell
curl --location --request POST 'http://localhost:8062/api/schedule/info/_/now/demo/quickstart' \
--header 'Content-Type: application/json' \
--data-raw '{
    "user_name": "Jane",
    "text": "Hello word"
}'
```
+ URL中info表示泛源名称, 与服务配置中要一致
+ _下划线表示trace_id, 本地服务中可以是任意字符
+ now表示运行时间, 由于采用httpSource, 则会调用同步接口, 立即执行
+ /demo/quickstart 是flow名称

## Flow埋点格式
以下是Flow的全埋点格式, 在生产环境中, 我们会对该数据再ETL转换后入库Hive. 这样的功能我们会计划开源
```json
{
    "end_time": 1646899876617,
    "event": { // 触发flow执行的event
        "argument": {
            "@": {
                "value": {
                    "text": "Hello world",
                    "user_name": "Jane"
                }
            }
        },
        "event_id": "6229b2a4fd18547ece6a793d",
        "flow_name": "/demo/quickstart",
        "trace_id": "f84f8fbb4c9636d1b1cc72229806e873"
    },
    "procedure": [ // 一个flow会触发很多procedure执行
        {
            "argument": { // procedure的输入参数
                "": {
                    "text": "Hello world",
                    "user_name": "Jane"
                }
            },
            "assign": { // Flow中赋值变量埋点
              "is_valid": true,
              "text_is_valid": true,
              "text_length": 11,
              "user_name": "Jane"
            },
            "choice": ["is_valid"], // Flow的分支选择tag
            "end_time": 1646899876602,
            "flow_id": "8e99c331d09ff0bc6c490b28da5cf2d4",
            "flow_name": "/demo/quickstart",
            "scope": ["main"], // 如果有子过程调用, 会有其他的scope
            "start_time": 1646899876571
        }
    ],
    "start_time": 1646899876540
}
```

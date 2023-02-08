# 设计理念

星盘是一种面向大数据+AI场景的模型编排框架。和该领域现存的其他框架不同，星盘的重点不在诸如特征处理、模型训练、容器部署等热门环节，
而是专注于组合多种基础AI能力来处理多模态流式数据。接下来，我们分别从大数据、AI、软件工程三个角度来阐述星盘的理念。

## 大数据

当人们同时提到大数据和AI这两个名词时，通常是指把大数据当做燃料，通过训练过程得到能力更强大的AI；而星盘的目标与此相反，
是希望能够**利用AI处理大数据**，特别是流式数据。 消息队列作为流式数据的存储介质，可以说是云原生时代最常用的中间件之一，
不仅是日志收容、IoT等场景的标准解决方案，同时也是一种常规的用于服务解耦的设计模式。不夸张的说，万事万物皆可放入队列；从这个意义上讲，
消息队列里的数据天生就是非结构化且多模态的。

而现有的流式处理框架，比如flink或者spark streaming，采用SQL或者类SQL为其核心抽象，分布式计算模型也脱胎于MapReduce，
非常适合处理结构化/半结构化数据上的统计类任务，并不适合处理非结构化多模态数据上的智能任务。以星盘孵化的场景为例，我们的流式数据源中包含服务日志、
边端传感器、 图像以及UGC数据，在处理过程中需要利用各种模型和算法对数据做加工处理，并在处理过程中和相关业务系统交互，其中部分环节还需要和人类交互。
目前主流的流式数据处理框架都无法胜任这种场景。

在IoT和云原生时代，人们会把越来越多的东西放进消息队列；随着AI能力的发展，也会有越来越多的人尝试用AI来处理这些数据。星盘就是为此而生。

## AI

一个用AI技术处理流式数据并做出响应的系统，毫无疑问属于智能系统，所以我们设计星盘时遵循经典的`感知-认知-决策-行动`范式:
* 感知，对应着消息队列中的数据流，代表着从外界输入给系统的源源不断的信号
* 认知，对应着各类数据服务和推理服务提供的能力，代表着把信号表征为信息的过程
* 决策，对应着系统中的控制流，代表着根据信息区分场景，触发不同分支的过程
* 行动，对应着对外部系统API的调用，代表着系统对外界进行影响和改变的过程

上述四个步骤还需要构成飞轮，也就是回路（Loop）；一个半自动的AI系统，应该能够在MLOps设施的帮助下方便的达成人在回路（Human in the loop）；
将来在AutoML技术的帮助下，星盘也有机会演进为智能化程度更高的框架。

星盘用下述概念来承载上述理念:
* 流程编制和编排，由FlowDSL进行描述，定义了星盘框架如何完成感知和决策过程。
* 从云计算的视角上看，星盘对自身的定位是网关，这意味着认知和行动的能力由其他微服务提供，星盘的功能是对这些外部能力进行组合(Composite)。
  因此这些外部能力在星盘中也被称为Compositor。
* 星盘内置了埋点和实验功能，这些用于提升飞轮和回路的运转速度。

这些功能具体的使用方式，请参考[开发者手册](../docs/programming-guides/developer-guide.md)

## 软件工程

星盘设计了专用的流程描述语言（FlowDSL），这里我们花一些篇幅来讲一下设计这门新语言的动机。当开始构建一个智能系统时，我们会问自己下述这些问题：
* 我们能不能通过一个EndToEnd模型来满足大部分需求？
* 我们构建的系统，是不是能够全自动运行，完全取代人类智能？
* 我们如何开发这么一个系统，是不是可以瀑布式开发，一次性交付？

基于对业界类似场景(比如无人车)的观察，以及自身的实践经验，我们相信在未来五到十年里全部有AI需求的场景中，有相当大比例会对这些问题say no。
然而我们的经验也表明，即使做不到端到端建模，做不到对人类智能的完全替代，也能够通过敏捷迭代持续产生可观的业务收益。得益于近年来AI技术的进步，
构造单一模态模型的门槛已经大大降低，无论这里的模态是指语音、视觉、文本还是结构化数据，也无论模型要完成的任务是分类、回归还是生成。
构造一个这样的模型，其难度和在传统软件工程实践中实现一个承载业务逻辑的函数也没有本质区别；这样的函数一旦被构造出来，如何融入整体的业务流程中，
也就变成了一个软件工程问题。

当然，引入这种通过AI技术得到的'函数'，会给整个系统带来额外的工作量，比如
* 现有AI技术得到的模型，多多少少会有一些黑盒性，需要额外注重***可观测性***建设，并设置兜底逻辑
* 我们需要在系统运转中得到新样本，定期重训模型，以对抗各种漂移问题
* 我们在引入新版本模型时，会先通过空跑或者AB实验来评价模型好坏，再进行放量

假设有下面这样的业务逻辑
``` python
def when_listen(something):
    if something == "吼叫":
      what = take_a_look()
      if what == "老虎":
        run_away()
```

如果我们要将上述的考虑实现，代码就会变成类似下述这样
``` python
def when_listen(trace_id, something, config):
    log(trace_id, something)
    if something == "吼叫":
      if config.use_new_version:
        what = take_a_better_look()
        # log ...
      else:
        what = take_a_look()
        # log ...
      if what == "老虎":
        result = run_away()
        add_label(result, something, what)
```

在软件工程领域，是通过AOP（面向切面编程）解决方案来应对这样的问题。只要我们还在利用编程的方法来构造智能系统，
MLOps的原则和理念就必然是开发过程中的一个极为重要的切面。通过设计一门专用的语言，我们不光获得了最大的自由度来植入MLOps功能，
将来也可以通过下面的代码，让类似可微编程的理念也能应用于更通用的场景
``` python
def when_listen(something):
    what = take_a_look()
    if smart_choice(something, what): # 指示系统根据运行结果自动生成最优的run_away策略
      run_away()
```

回顾现有大数据框架的发展历程，我们可以看到合适的语言对框架的普及有巨大的助力。MapReduce固然是极为优秀的抽象，
但与SQL的结合可以让非专业人员也能玩转大数据；类似的，在构建智能系统时我们也希望能找到一种像SQL那样可以被大众所接受的低代码语言。
我们认为BPM（业务流程管理）是一个可能的选项；当然，考虑到实现复杂度，目前我们没有选择实现一门符合BPMN标准的语言；不过让我们想象一下这种可能性：
一个本来是纯人工运转的流程体系，在框架的帮助下把一个个本来由人参与的节点自动化，快速的让AI技术落地到各行各业。
# Golang-gp_game_uin
## 实习的时候做的项目

#### 基于Golang开发，为平台添加小号登陆功能(账号与游戏角色由原来的一对一变为一对多), 微服务项目

#### 技术栈: Golang/MySQL/Redis/Thrift/Protocol Buffer/ETCD

#### 项目技术描述

采用的是公司自己平时开发的通用的一个框架

1. main.go 类似与SpringBoot的 App.java, 用于启动整个服务的主函数, 其中会调用所有的MySQL与Redis的读写初始化, Thrift的初始化, ETCD的初始化
2. manager 类似于MVC模式中的Controller层, 主要处理各种请求, 将请求分发到各个的Service层, 调用各种方法与逻辑请求
3. worker 类似于Service层, 用来实现业务逻辑, Controller需要调用响应的worker方法来出来请求的时候, 就会调用service层的东西, DO转化成VO也是在这里进行转化
4. 相应的中间件例如MySQL Redis需要使用专门的manager来调用相应的方法, 初始化中间件的方法是已经封装好了的相关方法, 只需要进行相应集群参数的填写, 即可进行初始化
5. read和write就是读写分离的概念, 增大负荷量与并发量
6. thrift_manager是专门针对服务间的调用写的manager, 若要调用其他的服务, 需要在该manager中写上相关服务的名字, ETCD会在注册中心寻找相应的服务名称来调用服务, 通过thrift传输封装好的protobuf序列化数据, 进行服务与服务之间的交互
7. sdk与dev仅仅只是分开的两个数据库的名字

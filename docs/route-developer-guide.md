# Route Developer Guide

cobarclient-2.0的路由结构采用了与cobarclient1.x不同的设计， 简化了实现，并强化了类型安全。

## Shard
每一个数据库分区现在在cobarclient2.0中使用Shard类型进行标识， Shard类似于cobarclient1.x中的DataSourceDescriptor， 用于提供标志性信息以及对应的DataSource引用，当然，也可以提供相应的描述性信息（可选）。

使用scala代码演示： `case class Shard(id:String, dataSource:DataSource, description:String="")`

<blockquote>
API是java实现，但文档中将使用scala代码来显示。
</blockquote>

Fucking Easy！

## Route

Route类型的概念类似于cobarclient1.x中的Rule，但简化很多，而且使用强类型应用来强调Typesafe的理念。 原则上，还是可以跟进路由规则的优先级将Route划分为cobarclient1.x中的四种Rule语义，不过，这些其实都可以归纳为一种类型来表示，从而诞生了现在的Route定义：

```scala
case class Route(sqlmap:String, expr:Expression, shards:Set[Shard]){
	def apply(action:String, argument:Any):Boolean = ...
}
```

也就是说， Route定义本质上不区分路由规则语义上的不同，这种语义上的不同将由Router类型来处理。

## Router

Router类型负责管理一组Route规则，并将他们分门别类划分层级和优先级， 当有路由请求的时候， Router将根据路由请求的上下文信息与自己持有的Route规则进行路由匹配， 其API抽象类似于：

```scala
trait Router{
	def route(sqlmapStatement:String, argument:Any): Set[Shard]
}
class SimpleRouter(routes: Set[Route]) extends Router{
	def route(sqlmapStatement:String, argument:Any): Set[Shard] = {...}
}
```

SimpleRouter是最基础的实现，如果不满足各位客官的需要，可以自己定制。

## How to glue them together?!

原则上， Shard, Route, Router都可以注册为Spring容器中的bean，然后依次依赖注入，怎么配置我就不用再说了吧？！ 各位肯定比我熟 ;-)

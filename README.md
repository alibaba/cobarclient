cobarclient
============


`mvn install` will package everything for you, including the documentations, the libraries, etc.

A quick-start can be found at <http://code.alibabatech.com/wiki/display/CobarClient/Home>

you can start with cobar-client by reading the docuemntation bundled with the distribution, the documentation is very elaborate.

GL & HF with cobar-client!



## About The Versions
- cobarclient1.x is for spring2.5.6 and ibatis2.3.x
- cobarclient2.x is for spring3.x and ibatis2.3.x
- cobarclient3.x is for spring3.x and mybatis 3.x

## Rule Definition Migration
cobarclient1.x的XML形式的Rule定义可以无缝的迁移到cobarclient2.x下，但核心包不做这个功能， 可以单独开辟一个子项目来做这个事情。

迁移的实现逻辑很简单， 将XML形式的Rule解析为Route实体Bean定义加载即可。 可以写一个Set<Route>类型的FactoryBean，也可以自定义类做转换， 总之，转化为Set<Route>形式的结果之后传给SimpleRouter作为构造函数即可， 在cobarclient2.x中，最基础的做法是直接在SpringContainer的配置文件中配置Set<Route>以及相应的Bean定义。




## Lisence 
<pre>
Apache License
Version 2.0, January 2004
http://www.apache.org/licenses/
</pre>
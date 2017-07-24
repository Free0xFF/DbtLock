# DbtLock
Distributed lock based on redis

## why?
公司的金融云，太蛋疼！redis只能2.4版本。而一些业务需要用到分布式锁，2.4版本的redis，并不支持Lua script语法，而python包Redlock的实现却用到了Lua script，即eval。Redlock算法最经典的地方正是使用Lua语法实现的，使用了eval函数。
基于上述原因，只能改掉Redlock的源码，去掉Lua语法部分，使用通俗常见的api实现，这样即使2.0版本的redis也可以使用redlock了。

### why java?
编写java版本，纯粹出于好玩的心态，练练手。经过测试，还是python版稳定可靠，三个终端，连续测试10万次，只有那么几次拿不到锁。而java就比较坑爹，但是可以调参数来改善，retryDelay，ttl(expire time)等等。

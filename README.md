# AsyncMemManager
An design for memory management which auto caching "cold" items, auto restore when item requested to save memory in async programing.

# Given
Async programming is key paradism in microservices, however nothing fit for all, and Async aslo have multi drawbacks, one of them is overflow memory. For example, in a very popular code template like this.

SomeClass obj = new SomeClass
obj.doSomeSetup()
CompletableFuture.runAsync(doSomeTimeConsumingJobs, executor)
                 .thenRunAsync(() -> {
                    obj.handleResult()
                 }); 
because obj is referred inside async task, so it's maintained in memory. If we queue big number of tasks like this, memory may be overflowed.
There couple of patterns to solve this issue, like throttling to limit number of queued tasks (so system may idle for awhile to wait for previous tasks done). 

# When
AsyncMemManager is a POC design of other way to solve this problem by manage those objects. The idea is wrapping those objects into containers and auto persist those objects lately referenced to save memory. When objects are looked up, AsyncMemManager auto restore them from storage if required.

# Then
Event with small memory capacity, almost un-limit number of task can be queued, and Async can be as is, no need complex design for throttling, re-cycle ...

# Demo
the POC include of 
- DemoErrorApp, this demo for very common Async code, which 2000 task would quickly got Memeory overflow exception when run with -Xmx64m (assume memory is limitted)
- asyncMemManager.server, this is Spring boot based async memCache server, it's not like others memCach like Rdis or memcached.org. It's specifically designed for AsyncMemManager which
 + Auto remove object after single retrieving 
 + No sharing loading between clients
 + Required specify expected TTL when storing
 + "Cold" data may be persisted to disk to save memory (similar to AsyncMemManager)
Note: To run asyncMemManager.server, we need folder to save data, which is currently hardcode as <USER_HOME>/async-caching
- DemoApp, this is aync code using AsyncMemManager, even with -Xmx64m, 10K tasks can bequeued and run properly.

# Detail Design (Comming soon)

   

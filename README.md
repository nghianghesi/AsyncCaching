# AsyncMemManager
A design for memory management which auto caching "cold" items to save memory, auto restore when item requested in async programing.

# Given
Async programming is key paradism in microservices, however nothing fit for all, and Async aslo have multi drawbacks, one of them is overflow memory. For example, in a very popular code template like this.

```java

SomeClass obj = new SomeClass
obj.doSomeSetup()
doSomeTimeConsumingJobs().thenRunAsync(() -> {
                    obj.handleResult()
                 }); 
// doSomeTimeConsumingJobs could be complex calculating, other API calling, database reading 
// which return a FutureCompletable (A promise in js, or a Task in .NET) ...
```              
              
because obj is referred inside async task, so it's maintained in memory. If we queue big number of tasks like this, memory may be overflowed.
There couple of patterns to solve this issue, like throttling to limit number of queued tasks (so system may idle for awhile to wait for previous tasks done). 

# When
AsyncMemManager is a POC design of other way to solve this problem by manage those objects. The idea is wrapping those objects into containers and auto persist those objects lately referenced to save memory. When objects are looked up, AsyncMemManager auto restore them from storage if required.

# Then
Even with just small memory capacity, almost un-limit number of tasks can be queued, and Async can be as is, no need complex design for throttling, re-circle tasks ...

# Demo
the POC include of 
- DemoErrorApp, this is demo for very common Async code, which 2000 task would quickly got Memeory overflow exception when run with -Xmx64m (assume memory is limitted)
- asyncMemManager.server, this is Spring boot based async memCache server, it's not like others memCach like Rdis or memcached.org. It's specifically designed for AsyncMemManager which
    + Auto remove object after single retrieving 
    + No sharing loading between clients
    + Required specify expected TTL when storing
    + "Cold" data may be persisted to disk to save memory (similar to AsyncMemManager)
    + Note: To run asyncMemManager.server, we need folder to save data, which is currently hardcode as <USER_HOME>/async-caching
- DemoApp, this is aync code using AsyncMemManager, even with -Xmx64m, 10K tasks can bequeued and run properly.

# Detail Design (Comming)
  + Object diagram
![AsyncMemManager](https://user-images.githubusercontent.com/46674635/123992309-2047e500-d991-11eb-9085-6da9d4f4742c.png)

  + AsyncMemManagement sequence
  ![AsyncMemManagerSequence](https://user-images.githubusercontent.com/46674635/124046257-5bb0d680-d9c6-11eb-94a7-87dbb773ef5a.jpg)


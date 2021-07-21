namespace DemoApp
{
    using System;
    using System.Collections.Generic;
    using System.Threading;
    using System.Threading.Tasks;

    using ASM = AsyncMemManager.Common;
    using ASMC = AsyncMemManager.Client;
    using DemoApp.model;
    class Program
    {
        static void Main(string[] args)
        {
            int capacity = 300 * TestEntity.LARGE_PROPERTY_SIZE;
            int initialSize = 20;
            int cleanupInterval = 3600;
            int candlePoolSize = 10;
            IDictionary<string, ASM.FlowKeyConfiguration> flowKeyConfig = new Dictionary<string, ASM.FlowKeyConfiguration>();
            ASM.Configuration config = new ASM.Configuration(capacity, initialSize, cleanupInterval, candlePoolSize, flowKeyConfig);
                    
            ASMC.DI.IPersistence memCachePersistence = new ASMC.MemCacheServerPersistence("http://localhost:8080/");
            ASMC.DI.IHotTimeCalculator hotTimeCalculator = new ASMC.AvgWaitTimeCalculator(500);
            ASMC.DI.IAsyncMemManager memManager = new ASMC.AsyncMemManagerImpl(config, hotTimeCalculator, memCachePersistence);

            DemoRESTClient demoClient = new DemoRESTClient("http://localhost:8080/");

            List<Task> tasks = new List<Task>();
            int n = 10000;
            for (int i=0; i<n; i++)
            {
                ASMC.DI.ISetupObject<TestEntity> setupEntity = memManager.Manage("DemoFlow", TestEntity.InitLargeObject(), TestEntity.TestEntityAsyncMemSerializer.Instance);
                ASMC.DI.IAsyncObject<TestEntity> e12 = setupEntity.AsyncO();
                ASMC.DI.IAsyncObject<TestEntity> e3 = setupEntity.AsyncO();
                int idx = i;

                var t = Task.Run(()=> demoClient.DoSomething());

                tasks.Add(t.ContinueWith((tr)=>{
                    e12.Close();

					e12.Apply((o) => {
						Console.WriteLine(tr.Result + " Then 2nd Async "+ idx +" "+ o.GetSomeText());
					});

                    Thread.Sleep(100 + new Random().Next(50));
                    Console.WriteLine(memManager.DebugInfo());
                }));

                tasks.Add(t.ContinueWith((tr)=>{
                    e3.Close();
                    					
					Console.WriteLine(tr.Result + " Then 3rd Async "+ idx + " " + e3.Supply((o) => o.GetSomeText()));

                    Thread.Sleep(150 + new Random().Next(50));
                    Console.WriteLine(memManager.DebugInfo());
                }));

                tasks.Add(DoSomethingOther(demoClient, setupEntity, idx)
                            .ContinueWith((_)=>{
                                Console.WriteLine("Other: " + memManager.DebugInfo());
                            }));                

                setupEntity.Close();
                Thread.Sleep(2 + new Random().Next(10));
            }

            Console.WriteLine("All task queued");
            foreach (Task t in tasks)
            {
                t.Wait();
            }
            Console.WriteLine("All task completed");
            Console.WriteLine(memManager.DebugInfo());
        }

        private static Task DoSomethingOther(DemoRESTClient demoClient, ASMC.DI.ISetupObject<TestEntity> setupEntity, int idx)
        {
            ASMC.DI.IAsyncObject<TestEntity> e = setupEntity.AsyncO();
            return Task.Run(()=> demoClient.DoSomeOtherthing()).ContinueWith((tr) => {
                            e.Close();
                                                
                            e.Apply((o) => {
                                Console.WriteLine(tr.Result + " Then 2nd Other "+ idx + " " + o.GetSomeText());
                            });
                            Thread.Sleep(150 + new Random().Next(50));
                        });
        }
    }
}

namespace AsyncMemManager.Client.DI
{
    using System;
    using System.Collections.Generic;

    public class Configuration
    {
        int initialSize;
        int capacity;
        int cleanupInterval;
        int candlePoolSize;
        IDictionary<string, FlowKeyConfiguration> flowKeyConfig = new Dictionary<string, FlowKeyConfiguration>();

        public Configuration(int capacity, 
                                int initialSize, 
                                int cleanupInterval,
                                int candlePoolSize,
                                IDictionary<string, FlowKeyConfiguration> flowKeyConfig) 
        {
            this.capacity = capacity;
            this.candlePoolSize = candlePoolSize >= 0 ? candlePoolSize : Environment.ProcessorCount;
            this.initialSize = initialSize > 0 ? initialSize : 100;
            this.cleanupInterval = cleanupInterval;
            this.flowKeyConfig = flowKeyConfig;
        }

        public int InitialSize => initialSize;

        public int Capacity => capacity;

        public int CleanupInterval => cleanupInterval;

        public int CandlePoolSize => candlePoolSize;

        public IDictionary<String, FlowKeyConfiguration> FlowKeyConfig => flowKeyConfig;
    }

    public class FlowKeyConfiguration
    {	
    }    
}
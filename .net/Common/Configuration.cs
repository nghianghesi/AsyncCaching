namespace AsyncMemManager.Client.DI
{
    public class Configuration
    {
        int initialSize;
        int capacity;
        int cleanupInterval;
        int candlePoolSize;
        IDictionary<String, FlowKeyConfiguration> flowKeyConfig = new HashMap<>();

        public Configuration(int capacity, 
                                int initialSize, 
                                int cleanupInterval,
                                int candlePoolSize,
                                IDictionary<String, FlowKeyConfiguration> flowKeyConfig) 
        {
            this.capacity = capacity;
            this.candlePoolSize = candlePoolSize >= 0 ? candlePoolSize : Runtime.getRuntime().availableProcessors();
            this.initialSize = initialSize > 0 ? initialSize : 100;
            this.cleanupInterval = cleanupInterval;
            this.flowKeyConfig = flowKeyConfig;
        }

        public int getInitialSize() {
            return initialSize;
        }

        public int getCapacity() {
            return capacity;
        }

        public int getCleanupInterval() {
            return cleanupInterval;
        }

        public int getCandlePoolSize() {
            return candlePoolSize;
        }

        public IDictionary<String, FlowKeyConfiguration> getFlowKeyConfig() {
            return flowKeyConfig;
        }	
    }

    public class FlowKeyConfiguration
    {	
    }    
}
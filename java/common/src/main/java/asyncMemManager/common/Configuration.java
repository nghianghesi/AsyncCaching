package asyncMemManager.common;

import java.util.HashMap;
import java.util.Map;

public class Configuration
{
	int initialSize;
	int capacity;
	int cleanupInterval;
	int candlePoolSize;
	Map<String, FlowKeyConfiguration> flowKeyConfig = new HashMap<>();

	public Configuration(int capacity, 
							int initialSize, 
							int cleanupInterval,
							int candlePoolSize,
							Map<String, FlowKeyConfiguration> flowKeyConfig) 
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

	public Map<String, FlowKeyConfiguration> getFlowKeyConfig() {
		return flowKeyConfig;
	}	
}

package asyncMemManager.client;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import asyncMemManager.client.di.*;
import asyncMemManager.common.Configuration;

public class AvgWaitTimeCalculator implements HotTimeCalculator{
	
	private Map<String, WaitTimeStats> stats = new ConcurrentHashMap<>();
	
	private long defaultWaitTime;
	public AvgWaitTimeCalculator(long defaultWaitTime) {
		this.defaultWaitTime = defaultWaitTime;
	}
	
	@Override
	public long calculate(Configuration config, String flowKey, int nth) {
		String statsKey = flowKey + nth;
		WaitTimeStats avg = this.stats.getOrDefault(statsKey, null);
		
		if (avg != null)
		{
			return avg.average;
		}else {
			return this.defaultWaitTime;
		}
	}

	@Override
	public void stats(Configuration config, String flowKey, int nth, long waittime) {
		String statsKey = flowKey + nth;
		WaitTimeStats avgNew = new WaitTimeStats();
		WaitTimeStats avg = this.stats.putIfAbsent(statsKey, avgNew);
		if(avg == null)
		{
			avg = avgNew;
		}
		
		int nextCount = avg.count + 1;
		synchronized (avg) {
			avg.average = (long) (1.0 * avg.average / nextCount * avg.count +  1.0 * waittime / nextCount);
			if (nextCount<5)
			{
				avg.count = nextCount;
			}
		}
	}
	
	private static class WaitTimeStats{
		int count;
		long average;
	}
}

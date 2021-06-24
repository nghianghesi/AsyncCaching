package asyncMemManager.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import asyncMemManager.common.di.HotTimeCalculator;

public class AvgWaitTimeCalculator implements HotTimeCalculator{
	
	private Map<String, WaitTimeStats> stats = new ConcurrentHashMap<>();
	
	private long defaultWaitTime;
	public AvgWaitTimeCalculator(long defaultWaitTime) {
		this.defaultWaitTime = defaultWaitTime;
	}
	
	@Override
	public long calculate(Configuration config, String flowKey, int nth, long waittime) {
		String statsKey = flowKey + nth;
		WaitTimeStats avgNew = new WaitTimeStats();
		WaitTimeStats avg = this.stats.putIfAbsent(statsKey, avgNew);
		if(avg == null)
		{
			avg = avgNew;
		}
		
		long nextCount = avg.count + 1;
		synchronized (avg) {
			avg.average = (long) (1.0 * avg.average / nextCount * avg.count +  1.0 * waittime / nextCount);
			if (nextCount<5)
			{
				avg.count = nextCount;
			}
		}
		
		statsKey = flowKey + (nth + 1);
		avg = this.stats.getOrDefault(statsKey, null);
		
		if (avg != null)
		{
			System.out.println("AvgWaitTimeCalculator " + nth + " " + avg.average);
			return avg.average;
		}else {
			return this.defaultWaitTime;
		}
	}
	
	private static class WaitTimeStats{
		long count;
		long average;
	}
}

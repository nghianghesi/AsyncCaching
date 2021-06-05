package asyncCache.common.di;

import asyncCache.common.SingleReadMemcache.Configuration;

public interface HotTimeCalculator {
	public void statsWaitingTime(String flowKey, long waitingTime);
	public long calculate(Configuration config, String flowKey);
}

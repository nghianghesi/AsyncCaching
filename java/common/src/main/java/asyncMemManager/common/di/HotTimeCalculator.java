package asyncMemManager.common.di;

import asyncMemManager.common.AsyncMemManager.Configuration;

public interface HotTimeCalculator {
	public void statsWaitingTime(String flowKey, long waitingTime);
	public long calculate(Configuration config, String flowKey);
}

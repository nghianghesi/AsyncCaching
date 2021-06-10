package asyncMemManager.common.di;

import asyncMemManager.common.Configuration;

public interface HotTimeCalculator {
	public void statsWaitingTime(String flowKey, long waitingTime);
	public long calculate(Configuration config, String flowKey);
}

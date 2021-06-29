package asyncMemManager.client.di;

import asyncMemManager.common.Configuration;

public interface HotTimeCalculator {
	public void stats(Configuration config, String flowKey, int nth, long waittime);
	public long calculate(Configuration config, String flowKey, int nth);
}

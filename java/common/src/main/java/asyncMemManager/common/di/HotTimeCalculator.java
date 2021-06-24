package asyncMemManager.common.di;

import asyncMemManager.common.Configuration;

public interface HotTimeCalculator {
	public long calculate(Configuration config, String flowKey, int nth, long waittime);
}

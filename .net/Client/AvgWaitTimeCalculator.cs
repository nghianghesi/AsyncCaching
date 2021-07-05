namespace AsyncMemManager.Client
{
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using AsyncMemManager.Common;

    public class AvgWaitTimeCalculator : DI.IHotTimeCalculator
    {
        private ConcurrentDictionary<string, WaitTimeStats> stats = new ConcurrentDictionary<string, WaitTimeStats>();
	    private long defaultWaitTime;

        public AvgWaitTimeCalculator(long defaultWaitTime) {
            this.defaultWaitTime = defaultWaitTime;
        }

        public void Stats(Configuration config, string flowKey, int nth, long waittime)
        {
            string statsKey = flowKey + nth;
            WaitTimeStats avg = this.stats.GetOrAdd(statsKey, new WaitTimeStats());
                       
            int nextCount = avg.count + 1;
            lock (avg) {
                avg.average = (long) (1.0 * avg.average / nextCount * avg.count +  1.0 * waittime / nextCount);
                if (nextCount<5)
                {
                    avg.count = nextCount;
                }
            }
        }

	    public long Calculate(Configuration config, string flowKey, int nth)
        {
            string statsKey = flowKey + nth;
            
            this.stats.TryGetValue(statsKey, out WaitTimeStats avg);            
            if (avg != null)
            {
                return avg.average;
            }else {
                return this.defaultWaitTime;
            }
        }


	
        private class WaitTimeStats{
            public int count;
            public long average;
        }        
    }
} 
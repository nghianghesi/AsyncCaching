namespace AsyncMemManager.Client.DI
{
    using System;

    public interface IAsyncMemManager
    {        
        public ISetupObject<T> Manage<T>(String flowKey, T obj, IAsyncMemSerializer<T> serializer) ;
        public String DebugInfo(); 
    }
	
	public interface ISetupObject<T> : IDisposable{
		public IAsyncObject<T> AsyncO();
		public T O();
	}
	
	public interface IAsyncObject<T> : IDisposable
	{		
		public R Supply<R>(Func<T,R> f);
		public void Apply(Action<T> f);
	}

    public interface IAsyncMemSerializer<T> {
        public string Serialize(T obj);
        public T Deserialize(string data);
        public long EstimateObjectSize(T obj);
    }

    public interface IHotTimeCalculator{
        public void Stats(Configuration config, string flowKey, int nth, long waittime);
	    public long Calculate(Configuration config, string flowKey, int nth);
    }
    
    public interface IPersistence {
	/**
	 * save data storage
	 * @param key
	 * @param data
	 * @return
	 */
	public void Store(Guid key, string data, long expectedDuration);
	
	/**
	 * retrieve and remove data from storage
	 * @param key
	 * @return
	 */
	public String Retrieve(Guid key);
	
	/**
	 * remove data from storage.
	 * @param key
	 */
	public void Remove(Guid key);
}
}

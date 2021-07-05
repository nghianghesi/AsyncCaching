namespace AsyncMemManager.Client
{
    using System;

    public class MemCacheServerPersistence : DI.IPersistence
    {
        private IAsyncCachingREST asyncCachingApi;
        public MemCacheServerPersistence(string asyncCachingUrl)
        {
            this.asyncCachingApi = Refit.RestService.For<IAsyncCachingREST>(asyncCachingUrl);
        }

		public void Store(Guid key, string data, long expectedDuration)
        {
            this.asyncCachingApi.Store(key, data, expectedDuration);
        }
		
		/**
		* retrieve and remove data from storage
		* @param key
		* @return
		*/
		public string Retrieve(Guid key)
        {
            return this.asyncCachingApi.Retrieve(key).Content;
        }
		
		/**
		* remove data from storage.
		* @param key
		*/
		public void Remove(Guid key)
        {
            this.asyncCachingApi.Remove(key);
        }
	
        public interface IAsyncCachingREST
        {
            [Refit.Post("/cache/{key}/{expectedDuration}")]
            public Refit.ApiResponse<object> Store([Refit.AliasAs("key")] Guid key, [Refit.Body] string data, [Refit.AliasAs("expectedDuration")] long expectedDuration);

            [Refit.Get("/cache/{key}")]
            public Refit.ApiResponse<string> Retrieve([Refit.AliasAs("key")] Guid key);

            [Refit.Delete("/cache/{key}")]
            public Refit.ApiResponse<object> Remove([Refit.AliasAs("key")]Guid key);
        }        
    }
}
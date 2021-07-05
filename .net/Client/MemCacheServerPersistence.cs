namespace AsyncMemManager.Client
{
    using System;
    using System.Threading.Tasks;
    using System.Net.Http;

    public class MemCacheServerPersistence : DI.IPersistence
    {
        private IAsyncCachingREST asyncCachingApi;
        public MemCacheServerPersistence(string asyncCachingUrl)
        {
            var httpClient = new HttpClient(new HttpClientDiagnosticsHandler(new HttpClientHandler())) { BaseAddress = new Uri(asyncCachingUrl) };

            this.asyncCachingApi = Refit.RestService.For<IAsyncCachingREST>(httpClient);
        }

		public void Store(Guid key, string data, long expectedDuration)
        {
            this.asyncCachingApi.Store(key, data, expectedDuration).Wait();
        }
		
		/**
		* retrieve and remove data from storage
		* @param key
		* @return
		*/
		public string Retrieve(Guid key)
        {
            return this.asyncCachingApi.Retrieve(key).Result;
        }
		
		/**
		* remove data from storage.
		* @param key
		*/
		public void Remove(Guid key)
        {
            this.asyncCachingApi.Remove(key).Wait();
        }
	
        public interface IAsyncCachingREST
        {
            [Refit.Post("/cache/{key}/{expectedDuration}")]
            public Task<string> Store([Refit.AliasAs("key")] Guid key, [Refit.Body] string data, [Refit.AliasAs("expectedDuration")] long expectedDuration);

            [Refit.Get("/cache/{key}")]
            public Task<string> Retrieve([Refit.AliasAs("key")] Guid key);

            [Refit.Delete("/cache/{key}")]
            public Task<string> Remove([Refit.AliasAs("key")]Guid key);
        }        
    }
}
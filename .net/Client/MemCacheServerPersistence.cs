namespace AsyncMemManager.Client
{
    using System;
    using System.Threading.Tasks;
    using RestSharp;

    public class MemCacheServerPersistence : DI.IPersistence
    {
        private RestClient client;

        public MemCacheServerPersistence(string asyncCachingUrl)
        {
            this.client = new RestClient(asyncCachingUrl);
        }

		public void Store(Guid key, string data, long expectedDuration)
        {
            var request = new RestRequest("/cache/{key}/{expectedDuration}")
                            .AddUrlSegment("key", key)
                            .AddUrlSegment("expectedDuration", expectedDuration)
                            .AddParameter("text/plain", data, ParameterType.RequestBody);
            client.Post(request);
        }
		
		/**
		* retrieve and remove data from storage
		* @param key
		* @return
		*/
		public string Retrieve(Guid key)
        {
            var request = new RestRequest("/cache/{key}")
                .AddUrlSegment("key", key);
            return client.Get<string>(request).Content;
        }
		
		/**
		* remove data from storage.
		* @param key
		*/
		public void Remove(Guid key)
        {
            var request = new RestRequest("/cache/{key}")
                .AddUrlSegment("key", key);
            this.client.Delete(request);
        }
    }
}
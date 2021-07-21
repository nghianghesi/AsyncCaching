namespace DemoApp
{
    using RestSharp;
    public class DemoRESTClient
    {
        
        private RestClient client;

        public DemoRESTClient(string demoAPIUrl)
        {
            this.client = new RestClient(demoAPIUrl);
            this.client.ThrowOnAnyError = true;
        }

		public string DoSomething()
        {
            var request = new RestRequest("/demo/dosomething");
            var res = client.Get<string>(request);            
            return res.Content;
        }

		public string DoSomeOtherthing()
        {
            var request = new RestRequest("/demo/dosomeotherthing");
            var res = client.Get<string>(request);            
            return res.Content;
        }
    }
}
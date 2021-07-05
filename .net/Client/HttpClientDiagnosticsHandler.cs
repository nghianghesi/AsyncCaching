namespace AsyncMemManager.Client
{
    using System;
    using System.Diagnostics;
    using System.Net.Http;
    using System.Threading;
    using System.Threading.Tasks;
    public class HttpClientDiagnosticsHandler: DelegatingHandler
    {

        public HttpClientDiagnosticsHandler(HttpMessageHandler innerHandler) : base(innerHandler)
        {
        }

        public HttpClientDiagnosticsHandler()
        {
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            if (request.Content != null && false)
            {
                var content = await request.Content.ReadAsStringAsync().ConfigureAwait(false);
                Console.WriteLine(string.Format("Request Content: {0}", content));
            }
            
            var response = base.Send(request, cancellationToken);
            return response;
        }
    }

}
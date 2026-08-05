using System.Net;
using System.Net.Http;

namespace ClickHouse.Driver.Http;

internal class DefaultHttpClientHandler : HttpClientHandler
{
    public DefaultHttpClientHandler(bool skipServerCertificateValidation)
    {
        // Deliberately off: see HttpHandlerProvider.CreateHandler for why the driver decodes responses
        // itself rather than letting the framework do it.
        AutomaticDecompression = DecompressionMethods.None;

        if (skipServerCertificateValidation)
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
        }
    }
}

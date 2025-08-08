using System.Net;
using System.Net.Http;

namespace ClickHouse.Driver.Http;

internal class DefaultHttpClientHandler : HttpClientHandler
{
    public DefaultHttpClientHandler(bool skipServerCertificateValidation)
    {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate;

        if (skipServerCertificateValidation)
        {
            ServerCertificateCustomValidationCallback = (_, _, _, _) => true;
        }
    }
}

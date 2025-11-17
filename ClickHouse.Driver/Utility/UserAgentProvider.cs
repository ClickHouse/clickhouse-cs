using System;
using System.Linq;
using System.Net;
using System.Net.Http.Headers;
using System.Reflection;
using System.Text;

namespace ClickHouse.Driver.Utility;

/// <summary>
/// Provides cached user agent information for HTTP headers.
/// System information is looked up once and cached for the lifetime of the application.
/// </summary>
internal static class UserAgentProvider
{
    private static readonly Lazy<CachedUserAgentInfo> LazyInfo = new Lazy<CachedUserAgentInfo>(() => new CachedUserAgentInfo());

    public static CachedUserAgentInfo Info => LazyInfo.Value;

    internal sealed class CachedUserAgentInfo
    {
        public CachedUserAgentInfo()
        {
            try
            {
                // Get assembly version
                string versionAndHash = Assembly
                    .GetExecutingAssembly()
                    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                    .InformationalVersion ?? "unknown";
                var version = versionAndHash.Split('+')[0];
                DriverProductInfo = new ProductInfoHeaderValue("ClickHouse.Driver", version);

                // Get OS information
                var osPlatform = Environment.OSVersion.Platform.ToString();
                var osDescription = ContainsNonAscii(System.Runtime.InteropServices.RuntimeInformation.OSDescription) // Some OSs have weird characters in here, which are not allowed in headers!
                    ? WebUtility.UrlEncode(System.Runtime.InteropServices.RuntimeInformation.OSDescription)
                    : System.Runtime.InteropServices.RuntimeInformation.OSDescription;
                var architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString();

                // Get runtime information
                var runtime = Environment.Version.ToString();

                // Pre-build ProductInfoHeaderValue objects
                SystemProductInfo = new ProductInfoHeaderValue($"(platform:{osPlatform}; os:{osDescription}; runtime:{runtime}; arch:{architecture})");
            }
            catch
            {
                // If anything fails during initialization, create fallback values
                DriverProductInfo ??= new ProductInfoHeaderValue("ClickHouse.Driver", "unknown");
                SystemProductInfo = new ProductInfoHeaderValue("(platform:unknown; os:unknown; runtime:unknown; arch:unknown)");
            }
        }

        private static bool ContainsNonAscii(string value)
        {
            return value.Any(c => (int)c > 0x7f);
        }

        /// <summary>
        /// Gets the driver ProductInfoHeaderValue (e.g., "ClickHouse.Driver/1.0.0").
        /// </summary>
        public ProductInfoHeaderValue DriverProductInfo { get; }

        /// <summary>
        /// Gets the system ProductInfoHeaderValue with platform, OS, runtime, and architecture information.
        /// </summary>
        public ProductInfoHeaderValue SystemProductInfo { get; }
    }
}

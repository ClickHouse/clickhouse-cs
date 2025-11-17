using System;
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
            // Get assembly version
            string versionAndHash = Assembly
                .GetExecutingAssembly()
                .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
                .InformationalVersion ?? "unknown";
            var version = versionAndHash.Split('+')[0];

            // Get OS information
            var osPlatform = Environment.OSVersion.Platform.ToString();
            var osDescription = System.Runtime.InteropServices.RuntimeInformation.OSDescription;
            var architecture = System.Runtime.InteropServices.RuntimeInformation.ProcessArchitecture.ToString();

            // Get runtime information
            var runtime = Environment.Version.ToString();

            // Pre-build ProductInfoHeaderValue objects
            DriverProductInfo = new ProductInfoHeaderValue("ClickHouse.Driver", version);
            SystemProductInfo = new ProductInfoHeaderValue($"(platform:{osPlatform}; os:{osDescription}; runtime:{runtime}; arch:{architecture})");
        }

        /// <summary>
        /// Gets the driver ProductInfoHeaderValue (e.g., "ClickHouse.Driver/1.0.0")
        /// </summary>
        public ProductInfoHeaderValue DriverProductInfo { get; }

        /// <summary>
        /// Gets the system ProductInfoHeaderValue with platform, OS, runtime, and architecture information
        /// </summary>
        public ProductInfoHeaderValue SystemProductInfo { get; }
    }
}

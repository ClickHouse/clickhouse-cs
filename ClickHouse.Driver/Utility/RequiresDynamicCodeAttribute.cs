#if !NET7_0_OR_GREATER
// net6.0 declares RequiresDynamicCodeAttribute internal; .NET 7 made it public. This copy lets the net6.0 build
// compile the annotations. The AOT analyzer does not run for net6.0, so it has no other effect there.
#pragma warning disable IDE0130 // Namespace does not match folder structure
namespace System.Diagnostics.CodeAnalysis;
#pragma warning restore IDE0130 // Namespace does not match folder structure

[AttributeUsage(AttributeTargets.Method | AttributeTargets.Constructor | AttributeTargets.Class, Inherited = false)]
internal sealed class RequiresDynamicCodeAttribute : Attribute
{
    public RequiresDynamicCodeAttribute(string message) => Message = message;

    public string Message { get; }

    public string Url { get; set; }
}
#endif

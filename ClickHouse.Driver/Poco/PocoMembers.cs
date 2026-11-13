using System.Diagnostics.CodeAnalysis;

namespace ClickHouse.Driver.Poco;

/// <summary>
/// The members of a POCO type that registration reads through reflection, so the trimmer keeps them.
/// </summary>
internal static class PocoMembers
{
    /// <summary>Binary insert reads the public properties.</summary>
    public const DynamicallyAccessedMemberTypes Insert = DynamicallyAccessedMemberTypes.PublicProperties;

    /// <summary>Read materialization also constructs the type through its public parameterless constructor.</summary>
    public const DynamicallyAccessedMemberTypes Read =
        DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.PublicParameterlessConstructor;
}

namespace ClickHouse.Driver.Tcp.Poco;

/// <summary>
/// How a POCO write plan reads the property of each row into the gather buffer of its column. Both tiers give the same
/// values, and the same first failure in row order, also when that is the failure of a property getter.
/// </summary>
internal enum PocoGatherTier
{
    /// <summary>
    /// One compiled loop for each column, which reads the property of each row. A runtime that compiles expression trees
    /// uses it.
    /// </summary>
    Compiled,

    /// <summary>
    /// A getter delegate for each row. It compiles no code, so a runtime that does not compile expression trees
    /// (<see cref="System.Runtime.CompilerServices.RuntimeFeature.IsDynamicCodeCompiled"/> is false) uses it. The plan still
    /// closes generic types at run time to build the converter trees.
    /// </summary>
    Delegate,
}

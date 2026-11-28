using System;
using System.Data;
using System.Data.Common;
using System.Threading;

namespace ClickHouse.Driver.ADO.Parameters;

/// <summary>
/// Represents a parameter for a <see cref="ClickHouseCommand"/>.
/// </summary>
/// <remarks>
/// Parameters are substituted using ClickHouse's native parameter syntax: <c>{name:Type}</c>.
/// </remarks>
public class ClickHouseDbParameter : DbParameter
{
    private string parameterName;
    private WeakReference<ClickHouseParameterCollection.NameIndex> nameObserver;

    /// <inheritdoc/>
    public override DbType DbType { get; set; }

    /// <summary>
    /// Gets or sets an explicit ClickHouse type name (e.g., "UInt64", "Nullable(String)").
    /// When set, this overrides automatic type inference from <see cref="Value"/>.
    /// </summary>
    public string ClickHouseType { get; set; }

    /// <summary>
    /// Gets the parameter direction. Always returns <see cref="ParameterDirection.Input"/>
    /// as ClickHouse only supports input parameters.
    /// </summary>
    public override ParameterDirection Direction { get => ParameterDirection.Input; set { } }

    /// <inheritdoc/>
    public override bool IsNullable { get; set; }

    /// <summary>
    /// Gets or sets the parameter name (without the @ prefix).
    /// </summary>
    public override string ParameterName
    {
        get
        {
            return parameterName;
        }

        set
        {
            var changed = parameterName != value;
            parameterName = value;
            if (changed && Volatile.Read(ref nameObserver)?.TryGetTarget(out var index) == true)
                index.Invalidate();
        }
    }

    // A parameter can appear in multiple collections. One index can observe it, and others can scan.
    internal bool TryObserveName(WeakReference<ClickHouseParameterCollection.NameIndex> observer)
    {
        var current = Volatile.Read(ref nameObserver);
        if (current == observer)
            return true;
        if (current?.TryGetTarget(out var index) == true && index.IsValid)
            return false;
        return Interlocked.CompareExchange(ref nameObserver, observer, current) == current;
    }

    /// <inheritdoc/>
    public override int Size { get; set; }

    /// <inheritdoc/>
    public override string SourceColumn { get; set; }

    /// <inheritdoc/>
    public override bool SourceColumnNullMapping { get; set; }

    /// <summary>
    /// Gets or sets the parameter value. The type is automatically inferred for SQL generation.
    /// </summary>
    public override object Value { get; set; }

    /// <inheritdoc/>
    public override void ResetDbType() { }

    /// <inheritdoc/>
    public override string ToString() => $"{ParameterName}:{Value}";

    /// <summary>
    /// Gets the ClickHouse parameter placeholder string to embed in SQL queries.
    /// Format: <c>{name:Type}</c> (e.g., <c>{id:Int32}</c>).
    /// </summary>
    /// <remarks>
    /// Uses default type inference only (explicit <see cref="ClickHouseType"/>, then built-in mapping).
    /// Does not account for <see cref="IParameterTypeResolver"/> or SQL type hints — those are
    /// applied internally by <c>ClickHouseClient</c> during query execution.
    /// </remarks>
    public string QueryForm
    {
        get
        {
            var typeName = ParameterTypeResolution.ResolveTypeName(this, null, null);
            return $"{{{ParameterName}:{typeName}}}";
        }
    }
}

using System;
using System.Collections.Concurrent;
using ClickHouse.Driver.Tcp.Poco;
using ClickHouse.Driver.Tcp.Tests.Utilities;

namespace ClickHouse.Driver.Tcp.Tests.Differential;

/// <summary>
/// The POCO read tier (<c>QueryAsync&lt;T&gt;</c>) as it was before it moved onto the converter derivation: the
/// reference of the differential tests (SPEC invariant 11). The old scatter is too large to copy here, so it stays in
/// production as <see cref="LegacyPocoColumnScatterFactory"/>, which only the tests call
/// (<see cref="PocoReadPlan{T}.BuildLegacy"/>). The old members that it calls (<see cref="PocoValueProjection"/>, the
/// codecs' <c>TryProjectColumnRead</c> and <c>TryProjectRead</c>) stay in production until the old path is removed.
/// </summary>
internal static class LegacyPocoRead
{
    /// <summary>The reference arm of the POCO tier: the old plan, read as the client arm reads.</summary>
    internal sealed class PocoArm : ReadArm
    {
        // Plans are cached by POCO type and block shape, as the client caches them, so a plan's column caches see the
        // blocks of more than one case.
        private static readonly ConcurrentDictionary<(Type PocoType, string Signature), object> Plans = new();

        public PocoArm(string name)
            : base(name, Tier.Poco)
        {
        }

        public override RowReader<T> Bind<T>(Block block)
        {
            var plan = (PocoReadPlan<Row<T>>)Plans.GetOrAdd(
                (typeof(Row<T>), PocoReadPlan.SignatureOf(block)),
                _ => PocoReadPlan<Row<T>>.BuildLegacy(PocoTypeDescriptor<Row<T>>.Build(), block));
            return (start, count) =>
            {
                var rows = new Row<T>[count];
                plan.Materialize(block, rows, start, count, rowOffset: start);
                return Array.ConvertAll(rows, row => row.Value);
            };
        }
    }
}

using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Tcp.Types;

namespace ClickHouse.Driver.Tcp.Tests.Types;

[TestFixture]
public class TupleShapeTests
{
    // The types that the C# compiler gives tuple literals: the reference for the nesting through TRest.
    private static IEnumerable<TestCaseData> LiteralTypes()
    {
        yield return new TestCaseData(1, typeof(ValueTuple<int>)).SetName("{m}(1)");
        yield return new TestCaseData(7, (1, 2, 3, 4, 5, 6, 7).GetType()).SetName("{m}(7)");
        yield return new TestCaseData(8, (1, 2, 3, 4, 5, 6, 7, 8).GetType()).SetName("{m}(8)");
        yield return new TestCaseData(9, (1, 2, 3, 4, 5, 6, 7, 8, 9).GetType()).SetName("{m}(9)");
        yield return new TestCaseData(14, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14).GetType()).SetName("{m}(14)");
        yield return new TestCaseData(15, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15).GetType()).SetName("{m}(15)");
        yield return new TestCaseData(16, (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16).GetType()).SetName("{m}(16)");
    }

    [TestCaseSource(nameof(LiteralTypes))]
    public void Of_IntElements_IsTheTypeOfTheCSharpLiteral(int count, Type expected)
        => Assert.That(TupleShape.Of(Enumerable.Repeat(typeof(int), count).ToArray()), Is.EqualTo(expected));

    [TestCaseSource(nameof(LiteralTypes))]
    public void TryGetElements_TypeOfTheCSharpLiteral_GivesEveryElement(int count, Type type)
    {
        bool found = TupleShape.TryGetElements(type, out Type[] elements);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(elements, Is.EqualTo(Enumerable.Repeat(typeof(int), count)));
        });
    }

    [Test]
    public void TryGetElements_EighthElementIsATuple_KeepsItAsOneElement()
    {
        // C# wraps a tuple in the eighth position, so that it is not taken for the elements after the seventh.
        Type type = (1, 2, 3, 4, 5, 6, 7, ("a", (byte)8)).GetType();

        bool found = TupleShape.TryGetElements(type, out Type[] elements);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(elements, Has.Length.EqualTo(8));
            Assert.That(elements[7], Is.EqualTo(typeof((string, byte))));
            Assert.That(TupleShape.Of(elements), Is.EqualTo(type));
        });
    }

    [Test]
    public void Of_MaxElements_RoundTripsThroughTryGetElements()
    {
        Type[] elements = Enumerable.Range(0, TupleShape.MaxElements).Select(i => i % 2 == 0 ? typeof(int) : typeof(string)).ToArray();

        bool found = TupleShape.TryGetElements(TupleShape.Of(elements), out Type[] back);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.True);
            Assert.That(back, Is.EqualTo(elements));
        });
    }

    [TestCase(typeof(int))]
    [TestCase(typeof(ValueTuple))]
    [TestCase(typeof(ValueTuple<,>))]
    [TestCase(typeof(Tuple<int, int>))]
    [TestCase(typeof(KeyValuePair<int, int>))]
    [TestCase(typeof(ValueTuple<int, int, int, int, int, int, int, int>))]
    [TestCase(typeof(ValueTuple<int, int, int, int, int, int, int, ValueTuple>))]
    [TestCase(typeof(ValueTuple<int, int, int, int, int, int, int, KeyValuePair<int, int>>))]
    public void TryGetElements_NoValueTupleOfElements_ReturnsFalse(Type type)
    {
        bool found = TupleShape.TryGetElements(type, out Type[] elements);

        Assert.Multiple(() =>
        {
            Assert.That(found, Is.False);
            Assert.That(elements, Is.Null);
        });
    }

    [Test]
    public void Of_NoElements_Throws()
        => Assert.Throws<ArgumentException>(() => TupleShape.Of(Array.Empty<Type>()));
}

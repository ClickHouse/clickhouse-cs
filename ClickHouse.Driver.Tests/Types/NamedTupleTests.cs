using System;
using System.Collections.Generic;
using System.Linq;
using ClickHouse.Driver.Types;
using NUnit.Framework;
using NUnit.Framework.Legacy;

namespace ClickHouse.Driver.Tests.Types;

[TestFixture]
public class NamedTupleTests
{
    [Test]
    public void Constructor_WithArrays_ShouldCreateValidTuple()
    {
        // Arrange
        var names = new[] { "name", "age", "active" };
        var values = new object[] { "Alice", 30, true };

        // Act
        var tuple = new NamedTuple(names, values);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(3));
            Assert.That(tuple["name"], Is.EqualTo("Alice"));
            Assert.That(tuple["age"], Is.EqualTo(30));
            Assert.That(tuple["active"], Is.EqualTo(true));
            Assert.That(tuple.Names, Is.EqualTo(names));
        });
    }

    [Test]
    public void Constructor_WithArrays_NullNames_ShouldThrowArgumentNullException()
    {
        // Arrange
        var values = new object[] { "Alice", 30 };

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => new NamedTuple(null, values));
        Assert.That(ex.ParamName, Is.EqualTo("names"));
    }

    [Test]
    public void Constructor_WithArrays_NullValues_ShouldThrowArgumentNullException()
    {
        // Arrange
        var names = new[] { "name", "age" };

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => new NamedTuple(names, null));
        Assert.That(ex.ParamName, Is.EqualTo("values"));
    }

    [Test]
    public void Constructor_WithArrays_MismatchedLengths_ShouldThrowArgumentException()
    {
        // Arrange
        var names = new[] { "name", "age" };
        var values = new object[] { "Alice" };

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => new NamedTuple(names, values));
        Assert.That(ex.Message, Does.Contain("Number of names must match number of values"));
    }

    [Test]
    public void Constructor_WithFields_ShouldCreateValidTuple()
    {
        // Arrange
        var fields = new[]
        {
            new object[] { "name", "Bob" },
            new object[] { "age", 25 },
            new object[] { "score", 95.5 }
        };

        // Act
        var tuple = new NamedTuple(fields);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(3));
            Assert.That(tuple["name"], Is.EqualTo("Bob"));
            Assert.That(tuple["age"], Is.EqualTo(25));
            Assert.That(tuple["score"], Is.EqualTo(95.5));
        });
    }

    [Test]
    public void Constructor_WithFields_NullFields_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => new NamedTuple((object[][])null));
        Assert.That(ex.ParamName, Is.EqualTo("fields"));
    }

    [Test]
    public void Constructor_WithFields_NullFieldAtIndex_ShouldThrowArgumentException()
    {
        // Arrange
        var fields = new[]
        {
            new object[] { "name", "Bob" },
            null,
            new object[] { "age", 25 }
        };

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => new NamedTuple(fields));
        Assert.That(ex.Message, Does.Contain("Field at index 1 must be an array of length 2"));
    }

    [Test]
    public void Constructor_WithFields_InvalidFieldLength_ShouldThrowArgumentException()
    {
        // Arrange
        var fields = new[]
        {
            new object[] { "name", "Bob" },
            new object[] { "age" }, // Only 1 element
            new object[] { "score", 95.5 }
        };

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => new NamedTuple(fields));
        Assert.That(ex.Message, Does.Contain("Field at index 1 must be an array of length 2"));
    }

    [Test]
    public void Constructor_WithFields_NonStringName_ShouldThrowArgumentException()
    {
        // Arrange
        var fields = new[]
        {
            new object[] { "name", "Bob" },
            new object[] { 123, 25 }, // Name is not a string
            new object[] { "score", 95.5 }
        };

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() => new NamedTuple(fields));
        Assert.That(ex.Message, Does.Contain("Field name at index 1 must be a string"));
    }

    [Test]
    public void Constructor_WithDictionary_ShouldCreateValidTuple()
    {
        // Arrange
        var dict = new Dictionary<string, object>
        {
            { "name", "Charlie" },
            { "age", 35 },
            { "balance", 1000.50m }
        };

        // Act
        var tuple = new NamedTuple(dict);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple.Length, Is.EqualTo(3));
            Assert.That(tuple["name"], Is.EqualTo("Charlie"));
            Assert.That(tuple["age"], Is.EqualTo(35));
            Assert.That(tuple["balance"], Is.EqualTo(1000.50m));
        });
    }

    [Test]
    public void Constructor_WithDictionary_NullDictionary_ShouldThrowArgumentNullException()
    {
        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => new NamedTuple((Dictionary<string, object>)null));
        Assert.That(ex.ParamName, Is.EqualTo("dictionary"));
    }

    [Test]
    public void Constructor_WithDictionary_EmptyDictionary_ShouldCreateEmptyTuple()
    {
        // Arrange
        var dict = new Dictionary<string, object>();

        // Act
        var tuple = new NamedTuple(dict);

        // Assert
        Assert.That(tuple.Length, Is.EqualTo(0));
    }

    [Test]
    public void IntIndexer_ValidIndex_ShouldReturnValue()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "first", "second", "third" },
            new object[] { 10, 20, 30 }
        );

        // Act & Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple[0], Is.EqualTo(10));
            Assert.That(tuple[1], Is.EqualTo(20));
            Assert.That(tuple[2], Is.EqualTo(30));
        });
    }

    [Test]
    public void IntIndexer_NegativeIndex_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "first", "second" },
            new object[] { 10, 20 }
        );

        // Act & Assert
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => _ = tuple[-1]);
        Assert.That(ex.ParamName, Is.EqualTo("index"));
    }

    [Test]
    public void IntIndexer_IndexTooLarge_ShouldThrowArgumentOutOfRangeException()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "first", "second" },
            new object[] { 10, 20 }
        );

        // Act & Assert
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => _ = tuple[2]);
        Assert.That(ex.ParamName, Is.EqualTo("index"));
    }

    [Test]
    public void StringIndexer_ValidName_ShouldReturnValue()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age", "city" },
            new object[] { "Alice", 30, "NYC" }
        );

        // Act & Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple["name"], Is.EqualTo("Alice"));
            Assert.That(tuple["age"], Is.EqualTo(30));
            Assert.That(tuple["city"], Is.EqualTo("NYC"));
        });
    }

    [Test]
    public void StringIndexer_NullName_ShouldThrowArgumentNullException()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act & Assert
        var ex = Assert.Throws<ArgumentNullException>(() => _ = tuple[null]);
        Assert.That(ex.ParamName, Is.EqualTo("name"));
    }

    [Test]
    public void StringIndexer_NonExistentName_ShouldThrowKeyNotFoundException()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act & Assert
        var ex = Assert.Throws<KeyNotFoundException>(() => _ = tuple["nonexistent"]);
        Assert.That(ex.Message, Does.Contain("Field 'nonexistent' not found"));
    }

    [Test]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(5)]
    [TestCase(10)]
    public void Length_ShouldReturnCorrectCount(int count)
    {
        // Arrange
        var names = Enumerable.Range(0, count).Select(i => $"field{i}").ToArray();
        var values = Enumerable.Range(0, count).Cast<object>().ToArray();
        var tuple = new NamedTuple(names, values);

        // Act & Assert
        Assert.That(tuple.Length, Is.EqualTo(count));
    }

    [Test]
    public void Names_ShouldReturnCorrectNames()
    {
        // Arrange
        var names = new[] { "alpha", "beta", "gamma" };
        var values = new object[] { 1, 2, 3 };
        var tuple = new NamedTuple(names, values);

        // Act
        var actualNames = tuple.Names;

        // Assert
        Assert.That(actualNames, Is.EqualTo(names));
    }

    [Test]
    public void Names_ShouldBeReadOnly()
    {
        // Arrange
        var names = new[] { "alpha", "beta" };
        var values = new object[] { 1, 2 };
        var tuple = new NamedTuple(names, values);

        // Act
        var actualNames = tuple.Names;

        // Assert - verify it's a readonly list (cannot be cast to mutable array)
        ClassicAssert.IsInstanceOf<IReadOnlyList<string>>(actualNames);
    }

    [Test]
    public void TryGetValue_ExistingName_ShouldReturnTrueAndValue()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age", "score" },
            new object[] { "Alice", 30, 95.5 }
        );

        // Act
        var result = tuple.TryGetValue("age", out var value);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.True);
            Assert.That(value, Is.EqualTo(30));
        });
    }

    [Test]
    public void TryGetValue_NonExistentName_ShouldReturnFalseAndNull()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act
        var result = tuple.TryGetValue("nonexistent", out var value);

        // Assert
        Assert.Multiple(() =>
        {
            Assert.That(result, Is.False);
            Assert.That(value, Is.Null);
        });
    }

    [Test]
    public void ContainsName_ExistingName_ShouldReturnTrue()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age", "city" },
            new object[] { "Alice", 30, "NYC" }
        );

        // Act & Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple.ContainsName("name"), Is.True);
            Assert.That(tuple.ContainsName("age"), Is.True);
            Assert.That(tuple.ContainsName("city"), Is.True);
        });
    }

    [Test]
    public void ContainsName_NonExistentName_ShouldReturnFalse()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act & Assert
        Assert.That(tuple.ContainsName("nonexistent"), Is.False);
    }

    [Test]
    public void ToString_ShouldFormatCorrectly()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age", "active" },
            new object[] { "Alice", 30, true }
        );

        // Act
        var result = tuple.ToString();

        // Assert
        Assert.That(result, Is.EqualTo("(name: Alice, age: 30, active: True)"));
    }

    [Test]
    public void ToString_EmptyTuple_ShouldReturnEmptyParentheses()
    {
        // Arrange
        var tuple = new NamedTuple(Array.Empty<string>(), Array.Empty<object>());

        // Act
        var result = tuple.ToString();

        // Assert
        Assert.That(result, Is.EqualTo("()"));
    }

    [Test]
    public void ToString_WithNullValue_ShouldHandleGracefully()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", null }
        );

        // Act
        var result = tuple.ToString();

        // Assert
        Assert.That(result, Is.EqualTo("(name: Alice, age: )"));
    }

    [Test]
    public void GetHashCode_SameValues_ShouldReturnSameHash()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act
        var hash1 = tuple1.GetHashCode();
        var hash2 = tuple2.GetHashCode();

        // Assert
        Assert.That(hash1, Is.EqualTo(hash2));
    }

    [Test]
    public void GetHashCode_DifferentValues_ShouldReturnDifferentHash()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Bob", 25 }
        );

        // Act
        var hash1 = tuple1.GetHashCode();
        var hash2 = tuple2.GetHashCode();

        // Assert
        Assert.That(hash1, Is.Not.EqualTo(hash2));
    }

    [Test]
    public void GetHashCode_WithNullValues_ShouldNotThrow()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age", "city" },
            new object[] { "Alice", null, "NYC" }
        );

        // Act & Assert
        Assert.DoesNotThrow(() => tuple.GetHashCode());
    }

    [Test]
    public void Equals_SameTuple_ShouldReturnTrue()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age", "active" },
            new object[] { "Alice", 30, true }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "age", "active" },
            new object[] { "Alice", 30, true }
        );

        // Act
        var result = tuple1.Equals(tuple2);

        // Assert
        Assert.That(result, Is.True);
    }

    [Test]
    public void Equals_DifferentLength_ShouldReturnFalse()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "age", "city" },
            new object[] { "Alice", 30, "NYC" }
        );

        // Act
        var result = tuple1.Equals(tuple2);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void Equals_DifferentNames_ShouldReturnFalse()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "years" },
            new object[] { "Alice", 30 }
        );

        // Act
        var result = tuple1.Equals(tuple2);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void Equals_DifferentValues_ShouldReturnFalse()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Bob", 25 }
        );

        // Act
        var result = tuple1.Equals(tuple2);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void Equals_NonNamedTupleObject_ShouldReturnFalse()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );
        var other = new { name = "Alice", age = 30 };

        // Act
        var result = tuple.Equals(other);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void Equals_NullObject_ShouldReturnFalse()
    {
        // Arrange
        var tuple = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act
        var result = tuple.Equals(null);

        // Assert
        Assert.That(result, Is.False);
    }

    [Test]
    public void Equals_WithNullValues_ShouldCompareCorrectly()
    {
        // Arrange
        var tuple1 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", null }
        );
        var tuple2 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", null }
        );
        var tuple3 = new NamedTuple(
            new[] { "name", "age" },
            new object[] { "Alice", 30 }
        );

        // Act & Assert
        Assert.Multiple(() =>
        {
            Assert.That(tuple1.Equals(tuple2), Is.True);
            Assert.That(tuple1.Equals(tuple3), Is.False);
        });
    }
}

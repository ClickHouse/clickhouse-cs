#nullable enable
// ReSharper disable InconsistentNaming

using System;
using System.Runtime.CompilerServices;

namespace ClickHouse.Driver.Vendor.K4os.Hash.xxHash
{
	/// <summary>
	/// Base class for both <see cref="XXH32"/> and <see cref="XXH64"/>. Do not use directly.
	/// </summary>
	internal unsafe class XXH
	{
		/// <summary>Protected constructor to prevent instantiation.</summary>
		protected XXH() { }

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static uint XXH_read32(void* p) => *(uint*) p;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static ulong XXH_read64(void* p) => *(ulong*) p;

		internal static void XXH_zero(void* target, int length) =>
			Unsafe.InitBlockUnaligned(target, 0, (uint) length);

		internal static void XXH_copy(void* target, void* source, int length) =>
			Unsafe.CopyBlockUnaligned(target, source, (uint) length);
		
		internal static void Validate(byte[] bytes, int offset, int length)
		{
			if (bytes == null || offset < 0 || length < 0 || offset + length > bytes.Length)
				throw new ArgumentException("Invalid buffer boundaries");
		}
	}
}

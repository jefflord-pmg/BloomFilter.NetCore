﻿using BloomFilter.Configurations;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;

namespace BloomFilter;

/// <summary>
/// Bloom Filter In Mempory Implement
/// </summary>
public class FilterMemory : Filter
{
    //The upper limit per bucket is 2147483640
    private BitArray[] _buckets;

    private readonly object sync = new();

    private static readonly ValueTask Empty = new();

    /// <summary>
    /// Initializes a new instance of the <see cref="FilterMemory"/> class.
    /// </summary>
    /// <param name="options"><see cref="FilterMemoryOptions"/></param>
    public FilterMemory(FilterMemoryOptions options)
        : base(options.Name, options.ExpectedElements, options.ErrorRate, HashFunction.Functions[options.Method])
    {

        if (options.Buckets is not null)
        {
            Import(options.Buckets);
        }
        else if (options.BucketBytes is not null)
        {
            Import(options.BucketBytes);
        }
        else if (options.Bits is not null)
        {
            if (options.BitsMore is not null)
            {
                Import([options.Bits, options.BitsMore]);
            }
            else
            {
                Import([options.Bits]);
            }
        }
        else if (options.Bytes is not null)
        {
            if (options.BytesMore is not null)
            {
                Import([options.Bytes, options.BytesMore]);
            }
            else
            {
                Import([options.Bytes]);
            }
        }
        else
        {
            Init();
        }
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FilterMemory"/> class.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="expectedElements">The expected elements.</param>
    /// <param name="errorRate">The error rate.</param>
    /// <param name="hashFunction">The hash function.</param>
    public FilterMemory(string name, long expectedElements, double errorRate, HashFunction hashFunction)
        : base(name, expectedElements, errorRate, hashFunction)
    {
        Init();
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="FilterMemory"/> class.
    /// </summary>
    /// <param name="name"></param>
    /// <param name="size">The size.</param>
    /// <param name="hashes">The hashes.</param>
    /// <param name="hashFunction">The hash function.</param>
    public FilterMemory(string name, long size, int hashes, HashFunction hashFunction)
        : base(name, size, hashes, hashFunction)
    {
        Init();
    }

    [MemberNotNull(nameof(_buckets))]
    private void Init()
    {
        var bits = new List<BitArray>();
        var m = Capacity;
        while (m > 0)
        {
            if (m > MaxInt)
            {
                bits.Add(new BitArray(MaxInt));
                m -= MaxInt;
            }
            else
            {
                bits.Add(new BitArray((int)m));
                break;
            }
        }
        _buckets = bits.ToArray();
    }

    /// <summary>
    /// Importing bitmap
    /// </summary>
    /// <param name="buckets">Sets the multiple bitmap</param>
    [MemberNotNull(nameof(_buckets))]
    public void Import(BitArray[] buckets)
    {
        if (buckets is null)
            throw new ArgumentNullException(nameof(buckets));

        if (buckets.Length == 0)
            throw new ArgumentOutOfRangeException($"The length must greater than 0", nameof(buckets));

        if (Capacity > buckets.Sum(s => (long)s.Length))
        {
            throw new ArgumentOutOfRangeException($"The length must less than or equal to {Capacity}", nameof(buckets));
        }

        lock (sync)
        {
            _buckets = new BitArray[buckets.Length];

            for (int i = 0; i < buckets.Length; i++)
            {
                _buckets[i] = new BitArray(buckets[i]);
            }
        }
    }

    /// <summary>
    /// Importing bitmap
    /// </summary>
    /// <param name="bits">Sets the bit value</param>
    /// <param name="bits2">Sets the bit value</param>
    [Obsolete("Use Import(BitArray[])")]
    [MemberNotNull(nameof(_buckets))]
    public void Import(BitArray bits, BitArray? bits2 = null)
    {
        if (bits2 is null)
        {
            Import([bits]);
        }
        else
        {
            Import([bits, bits2]);
        }
    }

    /// <summary>
    /// Importing bitmap
    /// </summary>
    /// <param name="bucketBytes">Sets the multiple bitmaps</param>
    [MemberNotNull(nameof(_buckets))]
    public void Import(IList<byte[]> bucketBytes)
    {
        if (bucketBytes is null)
            throw new ArgumentNullException(nameof(bucketBytes));

        if (bucketBytes.Count == 0)
            throw new ArgumentOutOfRangeException($"The length must greater than 0", nameof(bucketBytes));

        Import(bucketBytes.Select(s => new BitArray(s)).ToArray());
    }

    /// <summary>
    /// Importing bitmap
    /// </summary>
    /// <param name="bits">Sets the bit value</param>
    /// <param name="more">Sets more the bit value</param>
    [MemberNotNull(nameof(_buckets))]
    [Obsolete("Use Import(IList<byte[]>)")]
    public void Import(byte[] bits, byte[]? more = null)
    {
        if (more is null)
        {
            Import([bits]);
        }
        else
        {
            Import([bits, more]);
        }
    }


    /// <summary>
    /// Exporting bitmap
    /// </summary>
    public BitArray[] Export()
    {
        lock (sync)
        {
            return _buckets.Select(s => new BitArray(s)).ToArray();
        }
    }

    /// <summary>
    /// Exporting bitmap
    /// </summary>
    /// <param name="bits">Gets the bit value</param>
    /// <param name="more">Gets more the bit value</param>
    [Obsolete("Use Export()")]
    public void Export(out BitArray bits, out BitArray? more)
    {
        more = null;

        lock (sync)
        {
            bits = new BitArray(_buckets[0]);

            if (_buckets.Length > 1)
            {
                more = new BitArray(_buckets[1]);
            }
        }
    }



    /// <summary>
    /// Exporting bitmap
    /// </summary>
    public IList<byte[]> ExportToBytes()
    {
        int Mod(int len) => len % 8 > 0 ? 1 : 0;

        var result = new List<byte[]>();

        lock (sync)
        {
            foreach (var bucket in _buckets)
            {
                var bits = new byte[bucket.Length / 8 + Mod(bucket.Length)];
                bucket.CopyTo(bits, 0);
                result.Add(bits);
            }
        }

        return result;
    }

    /// <summary>
    /// Exporting bitmap
    /// </summary>
    /// <param name="bits">Gets the bit value</param>
    /// <param name="more">Gets more the bit value</param>
    [Obsolete("Use ExportToBytes()")]
    public void Export(out byte[] bits, out byte[]? more)
    {
        more = null;
        var result = ExportToBytes();
        bits = result[0];
        if (result.Count > 1)
        {
            more = result[1];
        }
    }

    /// <summary>
    /// Adds the passed value to the filter.
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public override bool Add(ReadOnlySpan<byte> data)
    {
        bool added = false;
        var positions = ComputeHash(data);
        lock (sync)
        {
            foreach (var position in positions)
            {
                if (!Get(position))
                {
                    added = true;
                    Set(position);
                }
            }
        }
        return added;
    }

    /// <summary>
    /// Adds the passed value to the filter.
    /// </summary>
    /// <param name="data"></param>
    /// <returns></returns>
    public override ValueTask<bool> AddAsync(ReadOnlyMemory<byte> data)
    {
        return new ValueTask<bool>(Add(data.Span));
    }

    public override IList<bool> Add(IEnumerable<byte[]> elements)
    {
        var hashes = new List<long>();
        foreach (var element in elements)
        {
            hashes.AddRange(ComputeHash(element));
        }

        var processResults = new bool[hashes.Count];
        lock (sync)
        {
            for (var i = 0; i < hashes.Count; i++)
            {
                if (!Get(hashes[i]))
                {
                    Set(hashes[i]);
                    processResults[i] = false;
                }
                else
                {
                    processResults[i] = true;
                }
            }
        }

        IList<bool> results = new List<bool>();
        bool wasAdded = false;
        int processed = 0;

        //For each value check, if all bits in ranges of hashes bits are set
        foreach (var item in processResults)
        {
            if (!item) wasAdded = true;
            if ((processed + 1) % Hashes == 0)
            {
                results.Add(wasAdded);
                wasAdded = false;
            }
            processed++;
        }

        return results;
    }

    public override ValueTask<IList<bool>> AddAsync(IEnumerable<byte[]> elements)
    {
        return new ValueTask<IList<bool>>(Add(elements));
    }

    /// <summary>
    /// Tests whether an element is present in the filter
    /// </summary>
    /// <param name="element"></param>
    /// <returns></returns>
    public override bool Contains(ReadOnlySpan<byte> element)
    {
        var positions = ComputeHash(element);
        lock (sync)
        {
            foreach (var position in positions)
            {
                if (!Get(position))
                    return false;
            }
        }
        return true;
    }

    public override ValueTask<bool> ContainsAsync(ReadOnlyMemory<byte> element)
    {
        return new ValueTask<bool>(Contains(element.Span));
    }

    public override IList<bool> Contains(IEnumerable<byte[]> elements)
    {
        var hashes = new List<long>();
        foreach (var element in elements)
        {
            hashes.AddRange(ComputeHash(element));
        }

        var processResults = new bool[hashes.Count];
        lock (sync)
        {
            for (var i = 0; i < hashes.Count; i++)
            {
                processResults[i] = Get(hashes[i]);
            }
        }

        IList<bool> results = new List<bool>();
        bool isPresent = true;
        int processed = 0;

        //For each value check, if all bits in ranges of hashes bits are set
        foreach (var item in processResults)
        {
            if (!item) isPresent = false;
            if ((processed + 1) % Hashes == 0)
            {
                results.Add(isPresent);
                isPresent = true;
            }
            processed++;
        }

        return results;
    }

    public override ValueTask<IList<bool>> ContainsAsync(IEnumerable<byte[]> elements)
    {
        return new ValueTask<IList<bool>>(Contains(elements));
    }

    public override bool All(IEnumerable<byte[]> elements)
    {
        return Contains(elements).All(e => e);
    }

    public override ValueTask<bool> AllAsync(IEnumerable<byte[]> elements)
    {
        return new ValueTask<bool>(All(elements));
    }

    /// <summary>
    /// Removes all elements from the filter
    /// </summary>
    public override void Clear()
    {
        lock (sync)
        {
            foreach (var item in _buckets)
            {
                item.SetAll(false);
            }
        }
    }

    public override ValueTask ClearAsync()
    {
        Clear();
        return Empty;
    }

    private void Set(long index)
    {
        int idx = LogMaxInt(index, out int mod);

        _buckets[idx].Set(mod, true);
    }

    public bool Get(long index)
    {
        int idx = LogMaxInt(index, out int mod);
        return _buckets[idx].Get(mod);
    }

    public override void Dispose()
    {
    }
}
﻿using System;
using System.Collections;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace BloomFilter;

public class DefaultFilterMemorySerializer : IFilterMemorySerializer
{
    public async ValueTask SerializeAsync(FilterMemorySerializerParam param, Stream stream)
    {
        byte[] nameBytes = Encoding.UTF8.GetBytes(param.Name ?? string.Empty);
        await stream.WriteAsync(BitConverter.GetBytes(nameBytes.Length), 0, 4);
        await stream.WriteAsync(nameBytes, 0, nameBytes.Length);

        await stream.WriteAsync(BitConverter.GetBytes(param.ExpectedElements), 0, 8);
        await stream.WriteAsync(BitConverter.GetBytes(param.ErrorRate), 0, 8);
        await stream.WriteAsync(BitConverter.GetBytes((int)param.Method), 0, 4);

        await stream.WriteAsync(BitConverter.GetBytes(param.Buckets.Length), 0, 4);
        foreach (var bucket in param.Buckets)
        {
            byte[] bucketBytes = new byte[bucket.Length / 8 + Mod(bucket.Length)];
            bucket.CopyTo(bucketBytes, 0);
            await stream.WriteAsync(BitConverter.GetBytes(bucketBytes.Length), 0, 4).ConfigureAwait(false);
            await stream.WriteAsync(bucketBytes, 0, bucketBytes.Length).ConfigureAwait(false);
        }
    }

    public async ValueTask<FilterMemorySerializerParam> DeserializeAsync(Stream stream)
    {
        var param = new FilterMemorySerializerParam();

        byte[] lengthBytes = new byte[4];
        byte[] int64Bytes = new byte[8];

        await ReadExactlyAsync(stream, lengthBytes);
        int nameLength = BitConverter.ToInt32(lengthBytes, 0);

        byte[] nameBytes = new byte[nameLength];
        await ReadExactlyAsync(stream, nameBytes);
        param.Name = Encoding.UTF8.GetString(nameBytes);

        await ReadExactlyAsync(stream, int64Bytes);
        param.ExpectedElements = BitConverter.ToInt64(int64Bytes, 0);

        await ReadExactlyAsync(stream, int64Bytes);
        param.ErrorRate = BitConverter.ToDouble(int64Bytes, 0);

        await ReadExactlyAsync(stream, lengthBytes);
        param.Method = (HashMethod)BitConverter.ToInt32(lengthBytes, 0);

        await ReadExactlyAsync(stream, lengthBytes);
        int bucketsLength = BitConverter.ToInt32(lengthBytes, 0);
        param.Buckets = new BitArray[bucketsLength];

        for (int i = 0; i < bucketsLength; i++)
        {
            await ReadExactlyAsync(stream, lengthBytes);
            int bitArrayLength = BitConverter.ToInt32(lengthBytes, 0);

            byte[] bucketBytes = new byte[bitArrayLength];
            await ReadExactlyAsync(stream, bucketBytes);

            param.Buckets[i] = new BitArray(bucketBytes);
        }

        return param;
    }

    private async Task ReadExactlyAsync(Stream stream, byte[] data)
    {
#if NET7_0_OR_GREATER
        await stream.ReadExactlyAsync(data, 0, data.Length).ConfigureAwait(false);
#else
        await stream.ReadAsync(data, 0, data.Length).ConfigureAwait(false);
#endif
    }

    private int Mod(int len) => len % 8 > 0 ? 1 : 0;
}
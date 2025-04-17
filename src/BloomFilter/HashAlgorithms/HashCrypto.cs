﻿using System;
using System.Collections;
using System.Security.Cryptography;

namespace BloomFilter.HashAlgorithms;

public class HashCryptoSHA1 : HashCrypto
{
    public override HashMethod Method => HashMethod.SHA1;

    public override long[] ComputeHash(ReadOnlySpan<byte> data, long m, int k)
    {
        using var hashAlgorithm = SHA1.Create();
        return ComputeHash(hashAlgorithm, data, m, k);
    }
}

public class HashCryptoSHA256 : HashCrypto
{
    public override HashMethod Method => HashMethod.SHA256;

    public override long[] ComputeHash(ReadOnlySpan<byte> data, long m, int k)
    {
        using var hashAlgorithm = SHA256.Create();
        return ComputeHash(hashAlgorithm, data, m, k);
    }
}

public class HashCryptoSHA384 : HashCrypto
{
    public override HashMethod Method => HashMethod.SHA384;

    public override long[] ComputeHash(ReadOnlySpan<byte> data, long m, int k)
    {
        using var hashAlgorithm = SHA384.Create();
        return ComputeHash(hashAlgorithm, data, m, k);
    }
}

public class HashCryptoSHA512 : HashCrypto
{
    public override HashMethod Method => HashMethod.SHA512;

    public override long[] ComputeHash(ReadOnlySpan<byte> data, long m, int k)
    {
        using var hashAlgorithm = SHA512.Create();
        return ComputeHash(hashAlgorithm, data, m, k);
    }
}

public abstract class HashCrypto : HashFunction
{
    protected long[] ComputeHash(HashAlgorithm hashAlgorithm, ReadOnlySpan<byte> data, long m, int k)
    {
        long[] positions = new long[k];

        int computedHashes = 0;

        byte[] digest = new byte[hashAlgorithm.HashSize / 8];
        byte[] output = new byte[hashAlgorithm.HashSize / 8];

#if !NET6_0_OR_GREATER
        var bytes = data.ToArray();
#endif
        while (computedHashes < k)
        {
            hashAlgorithm.TransformBlock(digest, 0, digest.Length, output, 0);

#if NET6_0_OR_GREATER
            hashAlgorithm.TryComputeHash(data, digest, out int bytesWritten);
#else

            digest = hashAlgorithm.ComputeHash(bytes, 0, bytes.Length);
#endif
            BitArray hashed = new(digest);

            int filterSize = 32 - (int)BinaryHelper.NumberOfLeadingZeros((uint)m);

            for (int split = 0; split < (hashAlgorithm.HashSize / filterSize) && computedHashes < k; split++)
            {
                int from = split * filterSize;
                int to = ((split + 1) * filterSize);

                int intHash = BinaryHelper.BitToIntOne(hashed, from, to);

                if (intHash < m)
                {
                    positions[computedHashes] = (uint)intHash;
                    computedHashes++;
                }
            }
        }

        return positions;
    }
}
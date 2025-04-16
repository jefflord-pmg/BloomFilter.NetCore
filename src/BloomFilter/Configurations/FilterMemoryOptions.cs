using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;

namespace BloomFilter.Configurations;

public class FilterMemoryOptions
{
    /// <summary>
    /// The Name
    /// </summary>
    public string Name { get; set; } = BloomFilterConstValue.DefaultInMemoryName;

    /// <summary>
    /// The expected elements
    /// </summary>
    public long ExpectedElements { get; set; } = 1000000;

    /// <summary>
    /// The error rate
    /// </summary>
    public double ErrorRate { get; set; } = 0.01;

    /// <summary>
    /// The Hash Method
    /// </summary>
    public HashMethod Method { get; set; } = HashMethod.Murmur3;

    /// <summary>
    /// Sets the bit value
    /// </summary>
    [Obsolete("Use Buckets")]
    public BitArray Bits { get; set; } = default!;

    /// <summary>
    /// Sets more the bit value
    /// </summary>
    [Obsolete("Use Buckets")]
    public BitArray? BitsMore { get; set; }

    /// <summary>
    /// Multiple bitmap
    /// </summary>
    public BitArray[]? Buckets { get; set; }

    /// <summary>
    /// Sets the bit value
    /// </summary>
    [Obsolete("Use BucketBytes")]
    public byte[] Bytes { get; set; } = default!;

    /// <summary>
    /// Sets more the bit value
    /// </summary>
    [Obsolete("Use BucketBytes")]
    public byte[]? BytesMore { get; set; }

    /// <summary>
    /// Multiple bitmap from bytes
    /// </summary>
    public IList<byte[]>? BucketBytes { get; set; }


    public byte[] Serialize()
    {
        using (MemoryStream ms = new MemoryStream())
        {
            // Write string Name
            byte[] nameBytes = System.Text.Encoding.UTF8.GetBytes(Name ?? string.Empty);
            ms.Write(BitConverter.GetBytes(nameBytes.Length), 0, 4);
            ms.Write(nameBytes, 0, nameBytes.Length);

            // Write numeric values
            ms.Write(BitConverter.GetBytes(ExpectedElements), 0, 8);
            ms.Write(BitConverter.GetBytes(ErrorRate), 0, 8);
            ms.Write(BitConverter.GetBytes((int)Method), 0, 4);

            // Serialize BitArray[] Buckets
            bool hasBuckets = Buckets != null && Buckets.Length > 0;
            ms.Write(BitConverter.GetBytes(hasBuckets), 0, 1);
            if (hasBuckets)
            {
                ms.Write(BitConverter.GetBytes(Buckets!.Length), 0, 4);
                foreach (var bucket in Buckets!)
                {
                    // For each BitArray, convert to byte[] and write
                    byte[] bucketBytes = new byte[(bucket.Length + 7) / 8];
                    bucket.CopyTo(bucketBytes, 0);
                    ms.Write(BitConverter.GetBytes(bucket.Length), 0, 4);
                    ms.Write(BitConverter.GetBytes(bucketBytes.Length), 0, 4);
                    ms.Write(bucketBytes, 0, bucketBytes.Length);
                }
            }

            // Serialize legacy BitArray Bits
            bool hasBits = Bits != null;
            ms.Write(BitConverter.GetBytes(hasBits), 0, 1);
            if (hasBits)
            {
                byte[] bitsBytes = new byte[(Bits.Length + 7) / 8];
                Bits.CopyTo(bitsBytes, 0);
                ms.Write(BitConverter.GetBytes(Bits.Length), 0, 4);
                ms.Write(BitConverter.GetBytes(bitsBytes.Length), 0, 4);
                ms.Write(bitsBytes, 0, bitsBytes.Length);
            }

            // Serialize legacy BitArray BitsMore
            bool hasBitsMore = BitsMore != null;
            ms.Write(BitConverter.GetBytes(hasBitsMore), 0, 1);
            if (hasBitsMore)
            {
                byte[] bitsMoreBytes = new byte[(BitsMore!.Length + 7) / 8];
                BitsMore.CopyTo(bitsMoreBytes, 0);
                ms.Write(BitConverter.GetBytes(BitsMore.Length), 0, 4);
                ms.Write(BitConverter.GetBytes(bitsMoreBytes.Length), 0, 4);
                ms.Write(bitsMoreBytes, 0, bitsMoreBytes.Length);
            }

            // Serialize IList<byte[]> BucketBytes
            bool hasBucketBytes = BucketBytes != null && BucketBytes.Count > 0;
            ms.Write(BitConverter.GetBytes(hasBucketBytes), 0, 1);
            if (hasBucketBytes)
            {
                ms.Write(BitConverter.GetBytes(BucketBytes!.Count), 0, 4);
                foreach (var bucketByte in BucketBytes!)
                {
                    ms.Write(BitConverter.GetBytes(bucketByte.Length), 0, 4);
                    ms.Write(bucketByte, 0, bucketByte.Length);
                }
            }

            // Serialize legacy byte[] Bytes
            bool hasBytes = Bytes != null && Bytes.Length > 0;
            ms.Write(BitConverter.GetBytes(hasBytes), 0, 1);
            if (hasBytes)
            {
                ms.Write(BitConverter.GetBytes(Bytes.Length), 0, 4);
                ms.Write(Bytes, 0, Bytes.Length);
            }

            // Serialize legacy byte[] BytesMore
            bool hasBytesMore = BytesMore != null && BytesMore.Length > 0;
            ms.Write(BitConverter.GetBytes(hasBytesMore), 0, 1);
            if (hasBytesMore)
            {
                ms.Write(BitConverter.GetBytes(BytesMore!.Length), 0, 4);
                ms.Write(BytesMore, 0, BytesMore.Length);
            }

            return ms.ToArray();
        }
    }

    /// <summary>
    /// Deserializes a byte array to a FilterMemoryOptions object
    /// </summary>
    /// <param name="data">The byte array containing serialized FilterMemoryOptions data</param>
    /// <returns>A deserialized FilterMemoryOptions object</returns>
    public static FilterMemoryOptions Deserialize(byte[] data)
    {
        FilterMemoryOptions options = new FilterMemoryOptions();

        using (MemoryStream ms = new MemoryStream(data))
        {
            byte[] lengthBytes = new byte[4];
            byte[] int64Bytes = new byte[8];

            // Read Name
            ms.Read(lengthBytes, 0, 4);
            int nameLength = BitConverter.ToInt32(lengthBytes, 0);
            byte[] nameBytes = new byte[nameLength];
            ms.Read(nameBytes, 0, nameLength);
            options.Name = System.Text.Encoding.UTF8.GetString(nameBytes);

            // Read numeric values
            ms.Read(int64Bytes, 0, 8);
            options.ExpectedElements = BitConverter.ToInt64(int64Bytes, 0);

            ms.Read(int64Bytes, 0, 8);
            options.ErrorRate = BitConverter.ToDouble(int64Bytes, 0);

            ms.Read(lengthBytes, 0, 4);
            options.Method = (HashMethod)BitConverter.ToInt32(lengthBytes, 0);

            // Deserialize BitArray[] Buckets
            byte[] boolBytes = new byte[1];
            ms.Read(boolBytes, 0, 1);
            bool hasBuckets = BitConverter.ToBoolean(boolBytes, 0);
            if (hasBuckets)
            {
                ms.Read(lengthBytes, 0, 4);
                int bucketsLength = BitConverter.ToInt32(lengthBytes, 0);
                options.Buckets = new BitArray[bucketsLength];

                for (int i = 0; i < bucketsLength; i++)
                {
                    ms.Read(lengthBytes, 0, 4);
                    int bitArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                    ms.Read(lengthBytes, 0, 4);
                    int byteArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                    byte[] bucketBytes = new byte[byteArrayLength];
                    ms.Read(bucketBytes, 0, byteArrayLength);

                    options.Buckets[i] = new BitArray(bucketBytes);
                    // Ensure the correct length since BitArray constructor from byte array might have 
                    // extra padding bits at the end
                    if (options.Buckets[i].Length != bitArrayLength)
                    {
                        BitArray resized = new BitArray(bitArrayLength);
                        for (int j = 0; j < bitArrayLength && j < options.Buckets[i].Length; j++)
                        {
                            resized[j] = options.Buckets[i][j];
                        }
                        options.Buckets[i] = resized;
                    }
                }
            }

            // Deserialize legacy BitArray Bits
            ms.Read(boolBytes, 0, 1);
            bool hasBits = BitConverter.ToBoolean(boolBytes, 0);
            if (hasBits)
            {
                ms.Read(lengthBytes, 0, 4);
                int bitArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                ms.Read(lengthBytes, 0, 4);
                int byteArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                byte[] bitsBytes = new byte[byteArrayLength];
                ms.Read(bitsBytes, 0, byteArrayLength);

                options.Bits = new BitArray(bitsBytes);
                // Ensure the correct length
                if (options.Bits.Length != bitArrayLength)
                {
                    BitArray resized = new BitArray(bitArrayLength);
                    for (int j = 0; j < bitArrayLength && j < options.Bits.Length; j++)
                    {
                        resized[j] = options.Bits[j];
                    }
                    options.Bits = resized;
                }
            }

            // Deserialize legacy BitArray BitsMore
            ms.Read(boolBytes, 0, 1);
            bool hasBitsMore = BitConverter.ToBoolean(boolBytes, 0);
            if (hasBitsMore)
            {
                ms.Read(lengthBytes, 0, 4);
                int bitArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                ms.Read(lengthBytes, 0, 4);
                int byteArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                byte[] bitsMoreBytes = new byte[byteArrayLength];
                ms.Read(bitsMoreBytes, 0, byteArrayLength);

                options.BitsMore = new BitArray(bitsMoreBytes);
                // Ensure the correct length
                if (options.BitsMore.Length != bitArrayLength)
                {
                    BitArray resized = new BitArray(bitArrayLength);
                    for (int j = 0; j < bitArrayLength && j < options.BitsMore.Length; j++)
                    {
                        resized[j] = options.BitsMore[j];
                    }
                    options.BitsMore = resized;
                }
            }

            // Deserialize IList<byte[]> BucketBytes
            ms.Read(boolBytes, 0, 1);
            bool hasBucketBytes = BitConverter.ToBoolean(boolBytes, 0);
            if (hasBucketBytes)
            {
                ms.Read(lengthBytes, 0, 4);
                int bucketBytesCount = BitConverter.ToInt32(lengthBytes, 0);

                options.BucketBytes = new List<byte[]>(bucketBytesCount);

                for (int i = 0; i < bucketBytesCount; i++)
                {
                    ms.Read(lengthBytes, 0, 4);
                    int byteArrayLength = BitConverter.ToInt32(lengthBytes, 0);

                    byte[] bucketBytes = new byte[byteArrayLength];
                    ms.Read(bucketBytes, 0, byteArrayLength);

                    options.BucketBytes.Add(bucketBytes);
                }
            }

            // Deserialize legacy byte[] Bytes
            ms.Read(boolBytes, 0, 1);
            bool hasBytes = BitConverter.ToBoolean(boolBytes, 0);
            if (hasBytes)
            {
                ms.Read(lengthBytes, 0, 4);
                int bytesLength = BitConverter.ToInt32(lengthBytes, 0);

                options.Bytes = new byte[bytesLength];
                ms.Read(options.Bytes, 0, bytesLength);
            }
            else
            {
                options.Bytes = new byte[0];
            }

            // Deserialize legacy byte[] BytesMore
            ms.Read(boolBytes, 0, 1);
            bool hasBytesMore = BitConverter.ToBoolean(boolBytes, 0);
            if (hasBytesMore)
            {
                ms.Read(lengthBytes, 0, 4);
                int bytesMoreLength = BitConverter.ToInt32(lengthBytes, 0);

                options.BytesMore = new byte[bytesMoreLength];
                ms.Read(options.BytesMore, 0, bytesMoreLength);
            }
        }

        return options;
    }
}
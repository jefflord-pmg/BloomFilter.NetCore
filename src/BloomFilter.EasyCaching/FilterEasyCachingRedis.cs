﻿using EasyCaching.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BloomFilter.EasyCaching
{
    /// <summary>
    /// Bloom Filter IRedisCachingProvider Implement
    /// </summary>
    public class FilterEasyCachingRedis : Filter
    {
        private readonly string _redisKey;
        private readonly IRedisCachingProvider _provider;

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterEasyCachingRedis"/> class.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="provider">The <see cref="IRedisCachingProvider"/>.</param>
        /// <param name="redisKey">The redisKey.</param>
        /// <param name="expectedElements">The expected elements.</param>
        /// <param name="errorRate">The error rate.</param>
        /// <param name="hashFunction">The hash function.</param>
        public FilterEasyCachingRedis(string name, IRedisCachingProvider provider, string redisKey, long expectedElements, double errorRate, HashFunction hashFunction)
            : base(name, expectedElements, errorRate, hashFunction)
        {
            if (string.IsNullOrWhiteSpace(redisKey)) throw new ArgumentException(nameof(redisKey));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _redisKey = redisKey;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="FilterEasyCachingRedis"/> class.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="provider">The <see cref="IRedisCachingProvider"/>.</param>
        /// <param name="redisKey">The redisKey.</param>
        /// <param name="capacity">The capacity.</param>
        /// <param name="hashes">The hashes.</param>
        /// <param name="hashFunction">The hash function.</param>
        public FilterEasyCachingRedis(string name, IRedisCachingProvider provider, string redisKey, long capacity, int hashes, HashFunction hashFunction)
            : base(name, capacity, hashes, hashFunction)
        {
            if (string.IsNullOrWhiteSpace(redisKey)) throw new ArgumentException(nameof(redisKey));
            _provider = provider ?? throw new ArgumentNullException(nameof(provider));
            _redisKey = redisKey;
        }

        public override bool Add(ReadOnlySpan<byte> element)
        {
            var positions = ComputeHash(element);
            return SetBit(positions).Any(a => !a);
        }

        public override async ValueTask<bool> AddAsync(ReadOnlyMemory<byte> data)
        {
            var positions = ComputeHash(data.Span);
            var results = await SetBitAsync(positions);
            return results.Any(a => !a);
        }

        public override IList<bool> Add(IEnumerable<byte[]> elements)
        {
            var addHashs = new List<long>();
            foreach (var element in elements)
            {
                addHashs.AddRange(ComputeHash(element));
            }

            IList<bool> results = new List<bool>();
            var processResults = SetBit(addHashs.ToArray());
            bool wasAdded = false;
            int processed = 0;
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

        public override async ValueTask<IList<bool>> AddAsync(IEnumerable<byte[]> elements)
        {
            var addHashs = new List<long>();
            foreach (var element in elements)
            {
                addHashs.AddRange(ComputeHash(element));
            }

            IList<bool> results = new List<bool>();
            var processResults = await SetBitAsync(addHashs.ToArray());
            bool wasAdded = false;
            int processed = 0;
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

        public override bool Contains(ReadOnlySpan<byte> element)
        {
            var positions = ComputeHash(element);
            return GetBit(positions).All(a => a);
        }

        public override async ValueTask<bool> ContainsAsync(ReadOnlyMemory<byte> element)
        {
            var positions = ComputeHash(element.Span);
            var results = await GetBitAsync(positions);
            return results.All(a => a);
        }

        public override IList<bool> Contains(IEnumerable<byte[]> elements)
        {
            var addHashs = new List<long>();
            foreach (var element in elements)
            {
                addHashs.AddRange(ComputeHash(element));
            }

            IList<bool> results = new List<bool>();

            var processResults = GetBit(addHashs.ToArray());
            bool isPresent = true;
            int processed = 0;
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

        public override async ValueTask<IList<bool>> ContainsAsync(IEnumerable<byte[]> elements)
        {
            var addHashs = new List<long>();
            foreach (var element in elements)
            {
                addHashs.AddRange(ComputeHash(element));
            }

            IList<bool> results = new List<bool>();

            var processResults = await GetBitAsync(addHashs.ToArray());
            bool isPresent = true;
            int processed = 0;
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

        public override bool All(IEnumerable<byte[]> elements)
        {
            return Contains(elements).All(e => e);
        }

        public override byte[] Serialize()
        {
            throw new NotImplementedException("Serialize is only for FilterMemory Class");
        }
        public override async ValueTask<bool> AllAsync(IEnumerable<byte[]> elements)
        {
            return (await ContainsAsync(elements)).All(e => e);
        }

        public override void Clear()
        {
            _provider.KeyDel(_redisKey);
        }

        public override async ValueTask ClearAsync()
        {
            await _provider.KeyDelAsync(_redisKey);
        }

        public override void Dispose()
        {
        }

        private IList<bool> SetBit(long[] positions)
        {
            var results = new bool[positions.Length];

            for (int i = 0; i < positions.Length; i++)
            {
                var result = _provider.Eval("return redis.call('SETBIT', KEYS[1], ARGV[1], 1)",
                    _redisKey, new object[] { positions[i] }.ToList());
                results[i] = result.ToString() == "1";
            }

            return results;
        }

        private async ValueTask<IList<bool>> SetBitAsync(long[] positions)
        {
            var results = new bool[positions.Length];

            for (int i = 0; i < positions.Length; i++)
            {
                var result = await _provider.EvalAsync("return redis.call('SETBIT', KEYS[1], ARGV[1], 1)",
                   _redisKey, new object[] { positions[i] }.ToList()).ConfigureAwait(false);
                results[i] = result.ToString() == "1";
            }

            return results;
        }

        private IList<bool> GetBit(long[] positions)
        {
            var results = new bool[positions.Length];

            for (int i = 0; i < positions.Length; i++)
            {
                var result = _provider.Eval("return redis.call('GETBIT', KEYS[1], ARGV[1])",
                    _redisKey, new object[] { positions[i] }.ToList());
                results[i] = result.ToString() == "1";
            }

            return results;
        }

        private async ValueTask<IList<bool>> GetBitAsync(long[] positions)
        {
            var results = new bool[positions.Length];

            for (int i = 0; i < positions.Length; i++)
            {
                var result = await _provider.EvalAsync("return redis.call('GETBIT', KEYS[1], ARGV[1])",
                    _redisKey, new object[] { positions[i] }.ToList()).ConfigureAwait(false);
                results[i] = result.ToString() == "1";
            }

            return results;
        }
    }
}
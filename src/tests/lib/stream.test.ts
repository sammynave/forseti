import { describe, it, expect, beforeEach } from 'vitest';
import { Stream, incrementalize } from '$lib/stream.js';
import { ZSet, ZSetGroup } from '$lib/z-set.js';
import { delay } from '$lib/operators/delay.js';
import { lift } from '$lib/operators/lift.js';

describe('Stream', () => {
	let g: ZSetGroup<string>;
	let emptyZSet: ZSet<string>;

	beforeEach(() => {
		g = new ZSetGroup();
		emptyZSet = g.zero();
	});

	describe('basic stream functionality', () => {
		it('should create stream with default value', () => {
			const defaultZSet = new ZSet([['default', 1]]);
			const stream = new Stream(defaultZSet);

			// Should return default value for any unset time
			expect(stream.at(0)).toEqual(defaultZSet);
			expect(stream.at(5)).toEqual(defaultZSet);
			expect(stream.at(100)).toEqual(defaultZSet);
		});

		it('should set and retrieve values at specific times', () => {
			const stream = new Stream(emptyZSet);
			const zset1 = new ZSet([['joe', 1]]);
			const zset2 = new ZSet([['anne', -1]]);

			stream.set(0, zset1);
			stream.set(3, zset2);

			// Should retrieve set values
			expect(stream.at(0)).toEqual(zset1);
			expect(stream.at(3)).toEqual(zset2);

			// Should return default for unset times
			expect(stream.at(1)).toEqual(emptyZSet);
			expect(stream.at(2)).toEqual(emptyZSet);
			expect(stream.at(4)).toEqual(emptyZSet);
		});
	});

	describe('stream properties', () => {
		it('should track current time correctly', () => {
			const stream = new Stream(emptyZSet);

			// Initially should be 0
			expect(stream.getCurrentTime()).toBe(0);

			stream.set(0, new ZSet([['a', 1]]));
			expect(stream.getCurrentTime()).toBe(1);

			stream.set(5, new ZSet([['b', 2]]));
			expect(stream.getCurrentTime()).toBe(6);

			// Setting earlier time shouldn't change current time
			stream.set(2, new ZSet([['c', 3]]));
			expect(stream.getCurrentTime()).toBe(6);
		});

		it('should iterate over entries correctly', () => {
			const stream = new Stream(emptyZSet);
			const zset1 = new ZSet([['a', 1]]);
			const zset2 = new ZSet([['b', 2]]);
			const zset3 = new ZSet([['c', 3]]);

			stream.set(0, zset1);
			stream.set(2, zset2);
			stream.set(5, zset3);

			const entries = Array.from(stream.entries());

			// Should contain all set entries
			expect(entries).toHaveLength(3);
			expect(entries).toContainEqual([0, zset1]);
			expect(entries).toContainEqual([2, zset2]);
			expect(entries).toContainEqual([5, zset3]);
		});

		it('should handle overwriting values at same time', () => {
			const stream = new Stream(emptyZSet);
			const zset1 = new ZSet([['first', 1]]);
			const zset2 = new ZSet([['second', 2]]);

			stream.set(0, zset1);
			stream.set(0, zset2); // Overwrite

			// Should have the latest value
			expect(stream.at(0)).toEqual(zset2);

			// Should only have one entry
			const entries = Array.from(stream.entries());
			expect(entries).toHaveLength(1);
			expect(entries[0]).toEqual([0, zset2]);
		});
	});

	describe('incrementalize function', () => {
		it('should implement Q^Δ = D ∘ Q ∘ I correctly', () => {
			// Simple query: identity (no transformation)
			const identityQuery = (stream: Stream<ZSet<string>>) => stream;

			const incrementalQuery = incrementalize(identityQuery, g, g);

			// Create delta stream
			const deltaStream = new Stream(emptyZSet);
			const delta1 = new ZSet([['joe', 1]]);
			const delta2 = new ZSet([['anne', 1]]);

			deltaStream.set(0, delta1);
			deltaStream.set(1, delta2);

			const result = incrementalQuery(deltaStream);

			// For identity query, incremental should return the deltas
			// D(I(delta)) = D(cumulative) = deltas
			expect(result.at(0)).toEqual(delta1);
			expect(result.at(1)).toEqual(delta2);
		});

		it('should handle non-trivial query transformations', () => {
			// Query that doubles all weights
			const doubleQuery = lift((zset: ZSet<string>) => zset.multiply(2));

			const incrementalQuery = incrementalize(doubleQuery, g, g);

			// Create delta stream
			const deltaStream = new Stream(emptyZSet);
			const delta = new ZSet([['item', 1]]);
			deltaStream.set(0, delta);

			const result = incrementalQuery(deltaStream);

			// Should apply the transformation incrementally
			// The delta gets integrated, doubled, then differentiated
			const expected = new ZSet([['item', 2]]);
			expect(result.at(0)).toEqual(expected);
		});

		it('should maintain incremental property across multiple updates', () => {
			// Query that filters positive weights
			const filterPositive = lift((zset: ZSet<string>) => {
				return new ZSet(zset.data.filter(([_, weight]) => weight > 0));
			});

			const incrementalQuery = incrementalize(filterPositive, g, g);

			// Create delta stream with mixed updates
			const deltaStream = new Stream(emptyZSet);
			const insert = new ZSet([
				['joe', 1],
				['anne', 1]
			]);
			const delete_joe = new ZSet([['joe', -1]]);
			const insert_bob = new ZSet([['bob', 1]]);

			deltaStream.set(0, insert);
			deltaStream.set(1, delete_joe);
			deltaStream.set(2, insert_bob);

			const result = incrementalQuery(deltaStream);

			// At time 0: insert joe and anne (both positive)
			expect(result.at(0)).toEqual(
				new ZSet([
					['joe', 1],
					['anne', 1]
				])
			);

			// At time 1: delete joe (should show removal)
			expect(result.at(1)).toEqual(new ZSet([['joe', -1]]));

			// At time 2: insert bob
			expect(result.at(2)).toEqual(new ZSet([['bob', 1]]));
		});

		it('should handle complex DBSP circuit composition', () => {
			// Create a more complex query: delay then double
			const delayThenDouble = (stream: Stream<ZSet<string>>) => {
				const delayed = delay(g)(stream);
				return lift((zset: ZSet<string>) => zset.multiply(2))(delayed);
			};

			const incrementalQuery = incrementalize(delayThenDouble, g, g);

			// Create delta stream
			const deltaStream = new Stream(emptyZSet);
			const delta1 = new ZSet([['a', 1]]);
			const delta2 = new ZSet([['b', 2]]);

			deltaStream.set(0, delta1);
			deltaStream.set(1, delta2);

			const result = incrementalQuery(deltaStream);

			// Due to delay, first delta appears at time 1, doubled
			expect(result.at(0)).toEqual(emptyZSet); // Default delayed and doubled
			expect(result.at(1)).toEqual(new ZSet([['a', 2]])); // delta1 delayed and doubled
			expect(result.at(2)).toEqual(new ZSet([['b', 4]])); // delta2 delayed and doubled
		});
	});
});

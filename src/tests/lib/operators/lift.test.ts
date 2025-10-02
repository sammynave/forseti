import { describe, it, expect, beforeEach } from 'vitest';
import { lift } from '$lib/operators/lift.js';
import { ZSet, ZSetGroup } from '$lib/z-set.js';
import { Stream } from '$lib/stream.js';
import { ZSetOperators } from '$lib/z-set-operators.js';

describe('lift', () => {
	let g: ZSetGroup<string>;
	let emptyZSet: ZSet<string>;

	beforeEach(() => {
		g = new ZSetGroup();
		emptyZSet = g.zero(); // Empty Z-set (group zero)
	});

	describe('basic lifting functionality', () => {
		it('should lift simple function over empty stream', () => {
			const input = new Stream(emptyZSet);
			// Function that adds a prefix to each element
			const addPrefix = (zset: ZSet<string>) => {
				return new ZSet(zset.data.map(([name, weight]) => [`prefix_${name}`, weight]));
			};

			const liftOp = lift(addPrefix);
			const output = liftOp(input);

			// Should apply function to default value (empty Z-set)
			expect(output.at(0)).toEqual(addPrefix(emptyZSet));
			expect(output.at(1)).toEqual(addPrefix(emptyZSet));
		});

		it('should lift function pointwise to each stream element', () => {
			const input = new Stream(emptyZSet);
			const zset1 = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			const zset2 = new ZSet([['bob', 2]]);

			input.set(0, zset1);
			input.set(1, zset2);

			// Function that doubles all weights
			const doubleWeights = (zset: ZSet<string>) => {
				return new ZSet(zset.data.map(([name, weight]) => [name, weight * 2]));
			};

			const liftOp = lift(doubleWeights);
			const output = liftOp(input);

			// Should apply function at each time point
			expect(output.at(0)).toEqual(doubleWeights(zset1));
			expect(output.at(1)).toEqual(doubleWeights(zset2));
		});
	});

	describe('DBSP core properties', () => {
		it('should satisfy pointwise application: (↑f)(s)[t] = f(s[t])', () => {
			const input = new Stream(emptyZSet);
			const zset0 = new ZSet([['a', 3]]);
			const zset1 = new ZSet([
				['b', -2],
				['c', 1]
			]);

			input.set(0, zset0);
			input.set(1, zset1);

			// Function that negates all weights
			const negateWeights = (zset: ZSet<string>) => g.negate(zset);

			const liftOp = lift(negateWeights);
			const output = liftOp(input);

			// Verify pointwise application
			expect(output.at(0)).toEqual(negateWeights(zset0));
			expect(output.at(1)).toEqual(negateWeights(zset1));
			expect(output.at(2)).toEqual(negateWeights(emptyZSet)); // Default value
		});

		it('should satisfy composition: ↑(g ∘ f) = ↑g ∘ ↑f', () => {
			const input = new Stream(emptyZSet);
			const zset = new ZSet([['item', 2]]);
			input.set(0, zset);

			// Two functions to compose
			const f = (zset: ZSet<string>) => new ZSet(zset.data.map(([name, w]) => [name, w + 1]));
			const g = (zset: ZSet<string>) => new ZSet(zset.data.map(([name, w]) => [name, w * 3]));
			const composed = (zset: ZSet<string>) => g(f(zset));

			// Test ↑(g ∘ f)
			const liftComposed = lift(composed);
			const outputComposed = liftComposed(input);

			// Test ↑g ∘ ↑f
			const liftF = lift(f);
			const liftG = lift(g);
			const intermediate = liftF(input);
			const outputChained = liftG(intermediate);

			// Should be equivalent
			expect(outputComposed.at(0)).toEqual(outputChained.at(0));
		});

		it('should handle identity function correctly', () => {
			const input = new Stream(emptyZSet);
			const zset = new ZSet([['test', 5]]);
			input.set(0, zset);

			// Identity function
			const identity = (x: ZSet<string>) => x;

			const liftOp = lift(identity);
			const output = liftOp(input);

			// Should be unchanged
			expect(output.at(0)).toEqual(zset);
			expect(output.at(1)).toEqual(emptyZSet);
		});

		it('should preserve linearity for linear functions', () => {
			const input1 = new Stream(emptyZSet);
			const input2 = new Stream(emptyZSet);
			const zset1 = new ZSet([['a', 1]]);
			const zset2 = new ZSet([['b', 2]]);

			input1.set(0, zset1);
			input2.set(0, zset2);

			// Linear function (scalar multiplication)
			const scaleBy2 = (zset: ZSet<string>) => zset.multiply(2);

			const liftOp = lift(scaleBy2);
			const output1 = liftOp(input1);
			const output2 = liftOp(input2);

			// Create combined input
			const inputCombined = new Stream(emptyZSet);
			inputCombined.set(0, g.add(zset1, zset2));
			const outputCombined = liftOp(inputCombined);

			// For linear functions: ↑f(a + b) = ↑f(a) + ↑f(b)
			const expected = g.add(output1.at(0), output2.at(0));
			expect(outputCombined.at(0)).toEqual(expected);
		});
	});

	describe('Z-set specific behavior', () => {
		it('should handle Z-set transformations correctly', () => {
			const input = new Stream(emptyZSet);
			const originalZSet = new ZSet([
				['joe', 1],
				['anne', -1],
				['bob', 2]
			]);
			input.set(0, originalZSet);

			// Function that filters positive weights and doubles them
			const filterAndDouble = (zset: ZSet<string>) => {
				const filtered = zset.data.filter(([_, weight]) => weight > 0);
				return new ZSet(filtered.map(([name, weight]) => [name, weight * 2]));
			};

			const liftOp = lift(filterAndDouble);
			const output = liftOp(input);

			// Should filter out anne (-1) and double joe (1→2) and bob (2→4)
			const expected = new ZSet([
				['joe', 2],
				['bob', 4]
			]);
			expect(output.at(0)).toEqual(expected);
		});

		it('should handle distinct operation through lifting', () => {
			const input = new Stream(emptyZSet);
			const mixedZSet = new ZSet([
				['a', 3],
				['b', -1],
				['c', 0],
				['d', 1]
			]);
			input.set(0, mixedZSet);

			// Lift the distinct operation
			// const liftDistinct = lift((zset: ZSet<string>) => zset.distinct());
			const liftDistinct = lift((zset: ZSet<string>) => ZSetOperators.distinct(zset));
			const output = liftDistinct(input);

			// Should convert to set: only positive weights become 1
			const expected = new ZSet([
				['a', 1],
				['d', 1]
			]);
			expect(output.at(0)).toEqual(expected);
		});
	});

	describe('stream integration', () => {
		it('should handle multiple time points with different transformations', () => {
			const input = new Stream(emptyZSet);
			const zset0 = new ZSet([['x', 1]]);
			const zset1 = new ZSet([['y', 2]]);
			const zset3 = new ZSet([['z', -1]]);

			// Set values with gaps
			input.set(0, zset0);
			input.set(1, zset1);
			input.set(3, zset3);

			// Function that adds timestamp as suffix
			const addTimestamp = (zset: ZSet<string>) => {
				return new ZSet(zset.data.map(([name, weight]) => [`${name}_transformed`, weight]));
			};

			const liftOp = lift(addTimestamp);
			const output = liftOp(input);

			// Verify transformation at each time point
			expect(output.at(0)).toEqual(new ZSet([['x_transformed', 1]]));
			expect(output.at(1)).toEqual(new ZSet([['y_transformed', 2]]));
			expect(output.at(2)).toEqual(addTimestamp(emptyZSet)); // Default
			expect(output.at(3)).toEqual(new ZSet([['z_transformed', -1]]));
		});
	});
});

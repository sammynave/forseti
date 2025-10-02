import { describe, it, expect, beforeEach } from 'vitest';
import { integrate } from '$lib/operators/integrate.js';
import { ZSet, ZSetGroup } from '$lib/z-set.js';
import { Stream } from '$lib/stream.js';

describe('integrate', () => {
	let g: ZSetGroup<string>;
	let emptyZSet: ZSet<string>;

	beforeEach(() => {
		g = new ZSetGroup();
		emptyZSet = g.zero(); // Empty Z-set (group zero)
	});

	describe('basic integration functionality', () => {
		it('should integrate empty stream correctly', () => {
			const input = new Stream(emptyZSet);
			// Input stream has no values set

			const intOp = integrate(g);
			const output = intOp(input);

			// All outputs should be zero since no input values
			expect(output.at(0)).toEqual(emptyZSet);
			expect(output.at(1)).toEqual(emptyZSet);
			expect(output.at(5)).toEqual(emptyZSet);
		});

		it('should integrate simple Z-set stream', () => {
			const input = new Stream(emptyZSet);
			const delta1 = new ZSet([['joe', 1]]);
			const delta2 = new ZSet([['anne', 1]]);

			input.set(0, delta1);
			input.set(1, delta2);

			const intOp = integrate(g);
			const output = intOp(input);

			// I(s)[0] = s[0] = {joe: 1}
			expect(output.at(0)).toEqual(delta1);

			// I(s)[1] = s[0] + s[1] = {joe: 1} + {anne: 1} = {joe: 1, anne: 1}
			const expected1 = g.add(delta1, delta2);
			expect(output.at(1)).toEqual(expected1);
		});
	});

	describe('DBSP core properties', () => {
		it('should satisfy I(s)[t] = Σ(i≤t) s[i] formula', () => {
			const input = new Stream(emptyZSet);
			const s0 = new ZSet([['a', 2]]);
			const s1 = new ZSet([['b', 1]]);
			const s2 = new ZSet([
				['a', -1],
				['c', 3]
			]);

			input.set(0, s0);
			input.set(1, s1);
			input.set(2, s2);

			const intOp = integrate(g);
			const output = intOp(input);

			// I(s)[0] = s[0] = {a: 2}
			expect(output.at(0)).toEqual(s0);

			// I(s)[1] = s[0] + s[1] = {a: 2} + {b: 1} = {a: 2, b: 1}
			const expected1 = g.add(s0, s1);
			expect(output.at(1)).toEqual(expected1);

			// I(s)[2] = s[0] + s[1] + s[2] = {a: 2, b: 1} + {a: -1, c: 3} = {a: 1, b: 1, c: 3}
			const expected2 = g.add(expected1, s2);
			expect(output.at(2)).toEqual(expected2);
		});

		it('should be linear: I(a + b) = I(a) + I(b)', () => {
			const inputA = new Stream(emptyZSet);
			const inputB = new Stream(emptyZSet);

			const deltaA0 = new ZSet([['x', 1]]);
			const deltaA1 = new ZSet([['x', 2]]);
			const deltaB0 = new ZSet([['y', 3]]);
			const deltaB1 = new ZSet([['y', -1]]);

			inputA.set(0, deltaA0);
			inputA.set(1, deltaA1);
			inputB.set(0, deltaB0);
			inputB.set(1, deltaB1);

			// Create combined input: a + b
			const inputCombined = new Stream(emptyZSet);
			inputCombined.set(0, g.add(deltaA0, deltaB0));
			inputCombined.set(1, g.add(deltaA1, deltaB1));

			const intOp = integrate(g);
			const outputA = intOp(inputA);
			const outputB = intOp(inputB);
			const outputCombined = intOp(inputCombined);

			// Verify I(a + b) = I(a) + I(b) at each time point
			expect(outputCombined.at(0)).toEqual(g.add(outputA.at(0), outputB.at(0)));
			expect(outputCombined.at(1)).toEqual(g.add(outputA.at(1), outputB.at(1)));
		});

		it('should handle cumulative nature correctly', () => {
			const input = new Stream(emptyZSet);
			const increment = new ZSet([['counter', 1]]);

			// Add same increment at multiple times
			input.set(0, increment);
			input.set(1, increment);
			input.set(2, increment);

			const intOp = integrate(g);
			const output = intOp(input);

			// Should accumulate: 1, 2, 3
			expect(output.at(0)).toEqual(new ZSet([['counter', 1]]));
			expect(output.at(1)).toEqual(new ZSet([['counter', 2]]));
			expect(output.at(2)).toEqual(new ZSet([['counter', 3]]));
		});
	});

	describe('Z-set specific behavior', () => {
		it('should handle Z-set addition correctly through integration', () => {
			const input = new Stream(emptyZSet);
			// Simulate database changes: insert joe, then delete anne, then insert anne
			const insert_joe = new ZSet([['joe', 1]]);
			const delete_anne = new ZSet([['anne', -1]]);
			const insert_anne = new ZSet([['anne', 1]]);

			input.set(0, insert_joe);
			input.set(1, delete_anne);
			input.set(2, insert_anne);

			const intOp = integrate(g);
			const output = intOp(input);

			// I(s)[0] = {joe: 1}
			expect(output.at(0)).toEqual(insert_joe);

			// I(s)[1] = {joe: 1} + {anne: -1} = {joe: 1, anne: -1}
			const expected1 = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			expect(output.at(1)).toEqual(expected1);

			// I(s)[2] = {joe: 1, anne: -1} + {anne: 1} = {joe: 1, anne: 0} = {joe: 1}
			// (anne cancels out to 0 and should be removed)
			const expected2 = new ZSet([['joe', 1]]);
			expect(output.at(2)).toEqual(expected2);
		});

		it('should preserve complex Z-set operations', () => {
			const input = new Stream(emptyZSet);
			// Use weights that will test merging behavior
			const delta1 = new ZSet([
				['item', 3],
				['other', 1]
			]);
			const delta2 = new ZSet([
				['item', -1],
				['new', 2]
			]);

			input.set(0, delta1);
			input.set(1, delta2);

			const intOp = integrate(g);
			const output = intOp(input);

			// I(s)[0] = {item: 3, other: 1}
			expect(output.at(0)).toEqual(delta1);

			// I(s)[1] = {item: 3, other: 1} + {item: -1, new: 2} = {item: 2, other: 1, new: 2}
			const expected = new ZSet([
				['item', 2],
				['other', 1],
				['new', 2]
			]);
			expect(output.at(1)).toEqual(expected);
		});
	});

	describe('stream integration', () => {
		it('should handle sparse streams with gaps correctly', () => {
			const input = new Stream(emptyZSet);
			const delta0 = new ZSet([['a', 1]]);
			const delta3 = new ZSet([['b', 2]]);
			const delta5 = new ZSet([['c', -1]]);

			// Set values with gaps: times 0, 3, 5
			input.set(0, delta0);
			input.set(3, delta3);
			input.set(5, delta5);

			const intOp = integrate(g);
			const output = intOp(input);

			// I(s)[0] = s[0] = {a: 1}
			expect(output.at(0)).toEqual(delta0);

			// I(s)[1] = s[0] + 0 = {a: 1} (no input at time 1)
			expect(output.at(1)).toEqual(emptyZSet);

			// I(s)[2] = s[0] + 0 + 0 = {a: 1} (no inputs at times 1,2)
			expect(output.at(2)).toEqual(emptyZSet);

			// I(s)[3] = s[0] + s[3] = {a: 1} + {b: 2} = {a: 1, b: 2}
			const expected3 = g.add(delta0, delta3);
			expect(output.at(3)).toEqual(expected3);

			// I(s)[4] = same as [3] since no input at time 4
			expect(output.at(4)).toEqual(emptyZSet);

			// I(s)[5] = s[0] + s[3] + s[5] = {a: 1, b: 2} + {c: -1} = {a: 1, b: 2, c: -1}
			const expected5 = g.add(expected3, delta5);
			expect(output.at(5)).toEqual(expected5);
		});
	});
});

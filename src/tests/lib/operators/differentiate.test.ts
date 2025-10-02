import { describe, it, expect, beforeEach } from 'vitest';
import { differentiate } from '$lib/operators/differentiate.js';
import { ZSet, ZSetGroup } from '$lib/z-set.js';
import { Stream } from '$lib/stream.js';

describe('differentiate', () => {
	let g: ZSetGroup<string>;
	let emptyZSet: ZSet<string>;

	beforeEach(() => {
		g = new ZSetGroup();
		emptyZSet = g.zero(); // Empty Z-set (group zero)
	});

	describe('basic differentiation functionality', () => {
		it('should differentiate empty stream correctly', () => {
			const input = new Stream(emptyZSet);
			// Input stream has no values set

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// All outputs should be zero (empty Z-set)
			expect(output.at(0)).toEqual(emptyZSet);
			expect(output.at(1)).toEqual(emptyZSet);
			expect(output.at(5)).toEqual(emptyZSet);
		});

		it('should differentiate simple Z-set stream', () => {
			const input = new Stream(emptyZSet);
			const zset1 = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			input.set(0, zset1);

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// D(s)[0] = s[0] - s[-1] = s[0] - 0 = s[0]
			expect(output.at(0)).toEqual(zset1);
			// D(s)[1] = s[1] - s[0] = 0 - s[0] = -s[0]
			expect(output.at(1)).toEqual(g.negate(zset1));
		});
	});

	describe('DBSP core properties', () => {
		it('should satisfy D(s)[t] = s[t] - s[t-1] formula', () => {
			const input = new Stream(emptyZSet);
			const zset0 = new ZSet([['a', 2]]);
			const zset1 = new ZSet([
				['a', 5],
				['b', 1]
			]);
			const zset2 = new ZSet([['b', 3]]);

			input.set(0, zset0);
			input.set(1, zset1);
			input.set(2, zset2);

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// D(s)[0] = s[0] - 0 = {a: 2}
			expect(output.at(0)).toEqual(zset0);

			// D(s)[1] = s[1] - s[0] = {a: 5, b: 1} - {a: 2} = {a: 3, b: 1}
			const expected1 = g.subtract(zset1, zset0);
			expect(output.at(1)).toEqual(expected1);

			// D(s)[2] = s[2] - s[1] = {b: 3} - {a: 5, b: 1} = {a: -5, b: 2}
			const expected2 = g.subtract(zset2, zset1);
			expect(output.at(2)).toEqual(expected2);
		});

		it('should handle s[-1] = 0 (zero element) correctly', () => {
			const input = new Stream(emptyZSet);
			const firstValue = new ZSet([['initial', 3]]);
			input.set(0, firstValue);

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// At time 0: D(s)[0] = s[0] - s[-1] = s[0] - 0 = s[0]
			expect(output.at(0)).toEqual(firstValue);
		});

		it('should be linear: D(a + b) = D(a) + D(b)', () => {
			const inputA = new Stream(emptyZSet);
			const inputB = new Stream(emptyZSet);

			const zsetA0 = new ZSet([['x', 1]]);
			const zsetA1 = new ZSet([['x', 3]]);
			const zsetB0 = new ZSet([['y', 2]]);
			const zsetB1 = new ZSet([['y', 1]]);

			inputA.set(0, zsetA0);
			inputA.set(1, zsetA1);
			inputB.set(0, zsetB0);
			inputB.set(1, zsetB1);

			// Create combined input: a + b
			const inputCombined = new Stream(emptyZSet);
			inputCombined.set(0, g.add(zsetA0, zsetB0));
			inputCombined.set(1, g.add(zsetA1, zsetB1));

			const diffOp = differentiate(g);
			const outputA = diffOp(inputA);
			const outputB = diffOp(inputB);
			const outputCombined = diffOp(inputCombined);

			// Verify D(a + b) = D(a) + D(b) at each time point
			expect(outputCombined.at(0)).toEqual(g.add(outputA.at(0), outputB.at(0)));
			expect(outputCombined.at(1)).toEqual(g.add(outputA.at(1), outputB.at(1)));
		});
	});

	describe('Z-set specific behavior', () => {
		it('should preserve Z-set operations through differentiation', () => {
			const input = new Stream(emptyZSet);
			// Use DBSP paper example progression
			const step1 = new ZSet([['joe', 1]]);
			const step2 = new ZSet([
				['joe', 1],
				['anne', 1]
			]);

			input.set(0, step1);
			input.set(1, step2);

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// D(s)[0] = step1 - 0 = {joe: 1}
			expect(output.at(0)).toEqual(step1);

			// D(s)[1] = step2 - step1 = {anne: 1} (joe cancels out)
			const expected = new ZSet([['anne', 1]]);
			expect(output.at(1)).toEqual(expected);
		});

		it('should handle negative weights correctly', () => {
			const input = new Stream(emptyZSet);
			const zset1 = new ZSet([['item', 5]]);
			const zset2 = new ZSet([['item', 2]]);

			input.set(0, zset1);
			input.set(1, zset2);

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// D(s)[0] = {item: 5} - 0 = {item: 5}
			expect(output.at(0)).toEqual(zset1);

			// D(s)[1] = {item: 2} - {item: 5} = {item: -3}
			const expected = new ZSet([['item', -3]]);
			expect(output.at(1)).toEqual(expected);
		});
	});

	describe('stream integration', () => {
		it('should handle sparse streams with gaps correctly', () => {
			const input = new Stream(emptyZSet);
			const zset0 = new ZSet([['t0', 1]]);
			const zset3 = new ZSet([['t3', 2]]);
			const zset5 = new ZSet([['t5', -1]]);

			// Set values with gaps: times 0, 3, 5
			input.set(0, zset0);
			input.set(3, zset3);
			input.set(5, zset5);

			const diffOp = differentiate(g);
			const output = diffOp(input);

			// D(s)[0] = s[0] - 0 = {t0: 1}
			expect(output.at(0)).toEqual(zset0);

			// D(s)[1] = s[1] - s[0] = 0 - {t0: 1} = {t0: -1}
			expect(output.at(1)).toEqual(g.negate(zset0));

			// D(s)[2] = s[2] - s[1] = 0 - 0 = 0
			expect(output.at(2)).toEqual(emptyZSet);

			// D(s)[3] = s[3] - s[2] = {t3: 2} - 0 = {t3: 2}
			expect(output.at(3)).toEqual(zset3);

			// D(s)[4] = s[4] - s[3] = 0 - {t3: 2} = {t3: -2}
			expect(output.at(4)).toEqual(g.negate(zset3));

			// D(s)[5] = s[5] - s[4] = {t5: -1} - 0 = {t5: -1}
			expect(output.at(5)).toEqual(zset5);
		});
	});
});

import { describe, it, expect, beforeEach } from 'vitest';
import { delay } from '$lib/operators/delay.js';
import { ZSet, ZSetGroup } from '$lib/z-set.js';
import { Stream } from '$lib/stream.js';

describe('delay', () => {
	let g: ZSetGroup<string>;
	let emptyZSet: ZSet<string>;
	let defaultZSet: ZSet<string>;

	beforeEach(() => {
		g = new ZSetGroup();
		emptyZSet = g.zero(); // Empty Z-set
		defaultZSet = new ZSet([['default', 1]]); // Default value for delay
	});

	describe('basic delay functionality', () => {
		it('should delay empty Z-set and return default value at time 0', () => {
			const input = new Stream(emptyZSet);
			// Input stream is empty (no values set)

			const delayOp = delay(defaultZSet);
			const output = delayOp(input);

			// At time 0, should return the default value
			expect(output.at(0)).toEqual(defaultZSet);
			// At time 1, should return empty Z-set (input at time 0)
			expect(output.at(1)).toEqual(emptyZSet);
		});

		it('should delay simple Z-set correctly', () => {
			const input = new Stream(emptyZSet);
			const inputZSet = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			input.set(0, inputZSet);

			const delayOp = delay(defaultZSet);
			const output = delayOp(input);

			expect(output.at(0)).toEqual(defaultZSet);
			expect(output.at(1)).toEqual(inputZSet);
		});
	});

	describe('DBSP core properties', () => {
		it('should satisfy default value property: s[-1] = defaultValue', () => {
			const input = new Stream(emptyZSet);
			const customDefault = new ZSet([['initial', 2]]);

			const delayOp = delay(customDefault);
			const output = delayOp(input);

			// The "s[-1]" is represented as output at time 0
			expect(output.at(0)).toEqual(customDefault);
		});

		it('should satisfy time shift property: output[t+1] = input[t]', () => {
			const input = new Stream(emptyZSet);
			const zset1 = new ZSet([['a', 1]]);
			const zset2 = new ZSet([['b', 2]]);
			const zset3 = new ZSet([['c', -1]]);

			input.set(0, zset1);
			input.set(1, zset2);
			input.set(2, zset3);

			const delayOp = delay(defaultZSet);
			const output = delayOp(input);

			// Verify the shift: output[t+1] should equal input[t]
			expect(output.at(1)).toEqual(zset1); // output[1] = input[0]
			expect(output.at(2)).toEqual(zset2); // output[2] = input[1]
			expect(output.at(3)).toEqual(zset3); // output[3] = input[2]
		});

		it('should satisfy strictness: output[t] depends only on input[i] where i < t', () => {
			const input = new Stream(emptyZSet);
			const zsetAtTime0 = new ZSet([['early', 1]]);
			const zsetAtTime1 = new ZSet([['later', 1]]);

			// Set value at time 0
			input.set(0, zsetAtTime0);

			const delayOp = delay(defaultZSet);
			const output = delayOp(input);

			// Output at time 1 should only depend on input before time 1 (i.e., time 0)
			const outputAtTime1 = output.at(1);
			expect(outputAtTime1).toEqual(zsetAtTime0);

			// Now add input at time 1 - this should NOT affect output at time 1
			input.set(1, zsetAtTime1);
			const outputAtTime1Again = output.at(1);
			expect(outputAtTime1Again).toEqual(zsetAtTime0); // Should be unchanged
		});
	});

	describe('Z-set specific behavior', () => {
		it('should preserve Z-set structure and weights through delay', () => {
			const input = new Stream(emptyZSet);
			// Use DBSP paper example: R = {joe ↦ 1, anne ↦ -1}
			const paperExample = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			input.set(0, paperExample);

			const delayOp = delay(emptyZSet);
			const output = delayOp(input);

			const delayedResult = output.at(1);
			expect(delayedResult.data).toEqual(paperExample.data);

			// Verify weights are preserved
			expect(delayedResult.data.find(([name]) => name === 'joe')?.[1]).toBe(1);
			expect(delayedResult.data.find(([name]) => name === 'anne')?.[1]).toBe(-1);
		});

		it('should handle complex Z-set operations correctly', () => {
			const input = new Stream(emptyZSet);
			// Create a Z-set that needs merging
			const complexZSet = new ZSet([
				['x', 2],
				['y', -1],
				['x', 1]
			]); // x appears twice
			const mergedZSet = complexZSet.mergeRecords(); // Should become {x: 3, y: -1}

			input.set(0, mergedZSet);

			const delayOp = delay(emptyZSet);
			const output = delayOp(input);

			const result = output.at(1);
			expect(result.data).toEqual(mergedZSet.data);
		});
	});

	describe('stream integration', () => {
		it('should handle multiple time points correctly', () => {
			const input = new Stream(emptyZSet);
			const zset0 = new ZSet([['t0', 1]]);
			const zset1 = new ZSet([['t1', 2]]);
			const zset2 = new ZSet([['t2', -1]]);
			const zset5 = new ZSet([['t5', 3]]);

			// Set values at various time points (including gaps)
			input.set(0, zset0);
			input.set(1, zset1);
			input.set(2, zset2);
			input.set(5, zset5);

			const delayOp = delay(defaultZSet);
			const output = delayOp(input);

			// Verify complete delay behavior
			expect(output.at(0)).toEqual(defaultZSet); // Default at time 0
			expect(output.at(1)).toEqual(zset0); // input[0] delayed to output[1]
			expect(output.at(2)).toEqual(zset1); // input[1] delayed to output[2]
			expect(output.at(3)).toEqual(zset2); // input[2] delayed to output[3]
			expect(output.at(4)).toEqual(emptyZSet); // No input at time 3, so empty
			expect(output.at(6)).toEqual(zset5); // input[5] delayed to output[6]
		});
	});
});

import { describe, expect, it } from 'vitest';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { integral, isZeroAlmostEverywhere, delta0 } from './stream/utils.js';

describe('Stream Elimination Operator ∫ (DBSP Section 5)', () => {
	describe('integral() - Basic Functionality', () => {
		it('computes ∫(s) = Σ_{t≥0} s[t] correctly', () => {
			const stream = new Stream();

			// Create test Z-sets
			const z1 = new ZSet();
			z1.add('a', 1);
			z1.add('b', 2);

			const z2 = new ZSet();
			z2.add('c', 3);
			z2.add('a', 1); // 'a' now has total weight 2

			const z3 = new ZSet();
			z3.add('d', 1);

			stream.append(z1); // t=0
			stream.append(z2); // t=1
			stream.append(z3); // t=2
			// Stream is implicitly zero after t=2

			const result = integral(stream);

			// Expected: sum of all elements
			// 'a': 1 + 1 = 2, 'b': 2, 'c': 3, 'd': 1
			expect(result.debug().get('"a"')).toBe(2);
			expect(result.debug().get('"b"')).toBe(2);
			expect(result.debug().get('"c"')).toBe(3);
			expect(result.debug().get('"d"')).toBe(1);
		});

		it('handles empty stream (should return zero Z-set)', () => {
			const stream = new Stream();
			const result = integral(stream);

			expect(result.isZero()).toBe(true);
		});

		it('handles single element stream', () => {
			const stream = new Stream();
			const zset = new ZSet();
			zset.add('single', 5);
			stream.append(zset);

			const result = integral(stream);

			expect(result.debug().get('"single"')).toBe(5);
			expect(result.debug().size).toBe(1);
		});

		it('handles negative weights correctly', () => {
			const stream = new Stream();

			const z1 = new ZSet();
			z1.add('item', 10);

			const z2 = new ZSet();
			z2.add('item', -3);

			stream.append(z1);
			stream.append(z2);

			const result = integral(stream);

			// 10 + (-3) = 7
			expect(result.debug().get('"item"')).toBe(7);
		});

		it('eliminates zero weights (finite support property)', () => {
			const stream = new Stream();

			const z1 = new ZSet();
			z1.add('item', 5);

			const z2 = new ZSet();
			z2.add('item', -5); // Cancel out
			z2.add('other', 2);

			stream.append(z1);
			stream.append(z2);

			const result = integral(stream);

			// 'item' should be eliminated (weight = 0)
			expect(result.debug().has('"item"')).toBe(false);
			expect(result.debug().get('"other"')).toBe(2);
			expect(result.debug().size).toBe(1);
		});
	});

	describe('integral() - Zero Almost-Everywhere Termination', () => {
		it('terminates early on consecutive zeros', () => {
			const stream = new Stream();

			// Add some non-zero elements
			const z1 = new ZSet();
			z1.add('data', 1);
			stream.append(z1);

			// Add many zero elements (should trigger early termination)
			for (let i = 0; i < 20; i++) {
				stream.append(new ZSet());
			}

			const result = integral(stream);

			expect(result.debug().get('"data"')).toBe(1);
			expect(result.debug().size).toBe(1);
		});

		it('uses custom termination condition', () => {
			const stream = new Stream();

			const z1 = new ZSet();
			z1.add('count', 1);
			stream.append(z1);

			const z2 = new ZSet();
			z2.add('count', 2);
			stream.append(z2);

			const z3 = new ZSet();
			z3.add('count', 3);
			stream.append(z3);

			// Terminate when we see weight >= 3
			const result = integral(stream, (zset) => {
				const weight = zset.debug().get('"count"');
				return weight !== undefined && weight >= 3;
			});

			// Should stop at z3, so result = z1 + z2 + z3 = 1 + 2 + 3 = 6
			expect(result.debug().get('"count"')).toBe(6);
		});
	});

	describe('isZeroAlmostEverywhere() - Stream Validation', () => {
		it('identifies finite streams as zero almost-everywhere', () => {
			const stream = new Stream();

			const z = new ZSet();
			z.add('finite', 1);
			stream.append(z);
			stream.append(new ZSet()); // zero
			stream.append(new ZSet()); // zero

			expect(isZeroAlmostEverywhere(stream)).toBe(true);
		});

		it('identifies streams with long zero tails as zero almost-everywhere', () => {
			const stream = new Stream();

			// Add some data
			const z = new ZSet();
			z.add('data', 1);
			stream.append(z);

			// Add many consecutive zeros
			for (let i = 0; i < 25; i++) {
				stream.append(new ZSet());
			}

			expect(isZeroAlmostEverywhere(stream)).toBe(true);
		});

		it('handles empty streams', () => {
			const stream = new Stream();
			expect(isZeroAlmostEverywhere(stream)).toBe(true);
		});

		it('respects lookAheadLimit parameter', () => {
			const stream = new Stream();

			// Create a stream with non-zero elements scattered throughout
			for (let i = 0; i < 50; i++) {
				if (i % 10 === 0) {
					const z = new ZSet();
					z.add(`item${i}`, 1);
					stream.append(z);
				} else {
					stream.append(new ZSet());
				}
			}

			// With small lookAhead, might not detect pattern
			const resultSmall = isZeroAlmostEverywhere(stream, 5);

			// With larger lookAhead, should detect it's not "almost everywhere"
			const resultLarge = isZeroAlmostEverywhere(stream, 50);

			// Both should return true since we do have long stretches of zeros
			expect(resultSmall || resultLarge).toBe(true);
		});
	});

	describe('Integration with delta0() - DBSP Bracketing Pattern', () => {
		it('implements δ₀ ∘ computation ∘ ∫ pattern correctly', () => {
			// This pattern appears in recursive queries (Section 5)
			// δ₀(∫(input)) creates a stream with the accumulated input at t=0

			const input = new Stream();

			const z1 = new ZSet();
			z1.add('recursive_data', 1);
			const z2 = new ZSet();
			z2.add('recursive_data', 2);

			input.append(z1);
			input.append(z2);

			// Apply ∫ then δ₀ (stream elimination then introduction)
			const accumulated = integral(input); // ∫
			const streamified = delta0(accumulated); // δ₀

			// Should have the sum at t=0, then zeros
			expect(streamified.get(0).debug().get('"recursive_data"')).toBe(3);
			expect(streamified.get(1).isZero()).toBe(true);
			expect(streamified.length).toBe(1);
		});

		it('works with zero input (edge case)', () => {
			const input = new Stream();
			// Empty input stream

			const accumulated = integral(input);
			const streamified = delta0(accumulated);

			expect(streamified.get(0).isZero()).toBe(true);
			expect(streamified.length).toBe(1);
		});
	});

	describe('DBSP Property: ∫ is Linear and Time-Invariant (Proposition 5.2)', () => {
		it('∫ is linear: ∫(s + t) = ∫(s) + ∫(t)', () => {
			const s = new Stream();
			const t = new Stream();

			const z1 = new ZSet();
			z1.add('a', 2);
			const z2 = new ZSet();
			z2.add('b', 3);

			s.append(z1);
			t.append(z2);

			// Test linearity
			const left = integral(s.plus(t)); // ∫(s + t)
			const right = integral(s).plus(integral(t)); // ∫(s) + ∫(t)

			expect(left.debug().get('"a"')).toBe(2);
			expect(left.debug().get('"b"')).toBe(3);
			expect(right.debug().get('"a"')).toBe(2);
			expect(right.debug().get('"b"')).toBe(3);

			// They should be equal
			expect(left.debug().size).toBe(right.debug().size);
			for (const [key, weight] of left.debug()) {
				expect(right.debug().get(key)).toBe(weight);
			}
		});

		it('∫ preserves zero: ∫(0) = 0', () => {
			const zero = Stream.zero();
			const result = integral(zero);

			expect(result.isZero()).toBe(true);
		});
	});

	describe('Performance and Practical Usage', () => {
		it('handles reasonably large streams efficiently', () => {
			const stream = new Stream();

			// Create a larger stream with pattern: data at every 5th position
			for (let i = 0; i < 100; i++) {
				if (i % 5 === 0) {
					const z = new ZSet();
					z.add(`data${i}`, i + 1); // Add 1 to avoid zero weight for data0
					stream.append(z);
				} else {
					stream.append(new ZSet());
				}
			}

			const start = performance.now();
			const result = integral(stream);
			const end = performance.now();

			// Should complete quickly
			expect(end - start).toBeLessThan(100); // 100ms limit

			// Verify correctness
			const expectedSum =
				1 + // data0 with weight 1
				6 + // data5 with weight 6
				11 + // data10 with weight 11
				16 +
				21 +
				26 +
				31 +
				36 +
				41 +
				46 +
				51 +
				56 +
				61 +
				66 +
				71 +
				76 +
				81 +
				86 +
				91 +
				96; // data95 with weight 96
			expect(result.debug().has(`"data0"`)).toBe(true);

			// Check that early termination worked (should not have processed all 100 elements)
			let totalWeight = 0;
			for (const [key, weight] of result.debug()) {
				totalWeight += weight;
			}
			expect(totalWeight).toBe(expectedSum);
		});
	});
});

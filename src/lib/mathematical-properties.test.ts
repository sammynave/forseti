import { describe, expect, it } from 'vitest';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { integrate } from './stream/utils.js';

describe('DBSP Mathematical Properties', () => {
	describe('Z-set Group Properties (Section 4.1)', () => {
		it('Z-sets form abelian group - associativity', () => {
			const z1 = new ZSet();
			const z2 = new ZSet();
			const z3 = new ZSet();

			z1.add('a', 1);
			z2.add('b', 2);
			z3.add('c', 3);

			// Test (z1 + z2) + z3 = z1 + (z2 + z3)
			const left = z1.plus(z2).plus(z3);
			const right = z1.plus(z2.plus(z3));

			expect(zsetsEqual(left, right)).toBe(true);
		});

		it('Z-sets form abelian group - commutativity', () => {
			const z1 = new ZSet();
			const z2 = new ZSet();

			z1.add('a', 1);
			z2.add('b', 2);

			// Test z1 + z2 = z2 + z1
			const left = z1.plus(z2);
			const right = z2.plus(z1);

			expect(zsetsEqual(left, right)).toBe(true);
		});

		it('Z-sets form abelian group - identity', () => {
			const z = new ZSet();
			z.add('a', 1);

			const zero = new ZSet();

			// Test z + 0 = z
			const result = z.plus(zero);
			expect(zsetsEqual(z, result)).toBe(true);
		});

		it('Z-sets form abelian group - inverse', () => {
			const z = new ZSet();
			z.add('a', 1);
			z.add('b', 2);

			// Test z + (-z) = 0
			const result = z.plus(z.negate());
			expect(result.isZero()).toBe(true);
		});

		it('Z-sets enforce finite support', () => {
			const z = new ZSet();
			z.add('a', 1);
			z.add('a', -1); // Should result in weight 0 and removal

			// Should not store zero weights
			expect(z.debug().has('"a"')).toBe(false);
			expect(z.isZero()).toBe(true);
		});
	});

	describe('Stream Group Properties (Proposition 2.11)', () => {
		it('streams form abelian group - associativity', () => {
			const s1 = new Stream();
			const s2 = new Stream();
			const s3 = new Stream();

			// Add test data
			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 1);
			const z3 = new ZSet();
			z3.add('c', 1);

			s1.append(z1);
			s2.append(z2);
			s3.append(z3);

			// Test (s1 + s2) + s3 = s1 + (s2 + s3)
			const left = s1.plus(s2).plus(s3);
			const right = s1.plus(s2.plus(s3));

			expect(streamsEqual(left, right)).toBe(true);
		});

		it('streams form abelian group - commutativity', () => {
			const s1 = new Stream();
			const s2 = new Stream();

			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 1);
			s1.append(z1);
			s2.append(z2);

			// Test s1 + s2 = s2 + s1
			const left = s1.plus(s2);
			const right = s2.plus(s1);

			expect(streamsEqual(left, right)).toBe(true);
		});

		it('streams form abelian group - identity', () => {
			const s = new Stream();
			const z = new ZSet();
			z.add('a', 1);
			s.append(z);

			const zero = Stream.zero();

			// Test s + 0 = s
			const result = s.plus(zero);
			expect(streamsEqual(s, result)).toBe(true);
		});

		it('streams form abelian group - inverse', () => {
			const s = new Stream();
			const z = new ZSet();
			z.add('a', 1);
			s.append(z);

			// Test s + (-s) = 0
			const result = s.plus(s.negate());
			expect(result.isZero()).toBe(true);
		});
	});

	describe('Differentiation and Integration (Definitions 2.15, 2.17)', () => {
		it('differentiation computes correct differences', () => {
			const s = new Stream();

			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('a', 1);
			z2.add('b', 1);
			s.append(z1);
			s.append(z2);

			const diff = s.differentiate();

			// D(s)[0] = s[0] - 0 = s[0]
			expect(zsetsEqual(diff.get(0), z1)).toBe(true);

			// D(s)[1] = s[1] - s[0] = just 'b'
			const expected = new ZSet();
			expected.add('b', 1);
			expect(zsetsEqual(diff.get(1), expected)).toBe(true);
		});

		it('integration and differentiation are inverses (Theorem 2.20)', () => {
			const s = new Stream();
			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 1);
			s.append(z1);
			s.append(z2);

			// Test I(D(s)) = s
			const integrated = integrate(s.differentiate());
			expect(streamsEqual(integrated, s)).toBe(true);

			// Test D(I(s)) = s
			const differentiated = integrate(s).differentiate();
			expect(streamsEqual(differentiated, s)).toBe(true);
		});
	});

	describe('Theorem 3.4 - Bilinear Operators', () => {
		it('incremental join produces correct results', () => {
			const s1 = new Stream();
			const s2 = new Stream();

			// Create test data
			const z1 = new ZSet();
			z1.add({ id: 1, name: 'A' }, 1);
			const z2 = new ZSet();
			z2.add({ id: 1, value: 100 }, 1);
			s1.append(z1);
			s2.append(z2);

			// Incremental approach: Theorem 3.4
			const result = s1.liftJoinIncremental(
				s2,
				(x) => x.id,
				(y) => y.id
			);

			// Should produce the joined result at time 0
			const expected = new ZSet();
			expected.add(
				[
					{ id: 1, name: 'A' },
					{ id: 1, value: 100 }
				],
				1
			);
			expect(zsetsEqual(result.get(0), expected)).toBe(true);

			// Time 1 should be empty (due to delay operation)
			expect(result.get(1).isZero()).toBe(true);
		});

		it('verifies Theorem 3.4 formula directly', () => {
			const a = new Stream();
			const b = new Stream();

			const za = new ZSet();
			za.add({ id: 1, x: 'A' }, 1);
			const zb = new ZSet();
			zb.add({ id: 1, y: 'B' }, 1);
			a.append(za);
			b.append(zb);

			// Manual calculation of (a × b)^Δ = I(a) × b + a × z^(-1)(I(b))
			const integratedA = integrate(a);
			const integratedB = integrate(b);
			const delayedIntegratedB = integratedB.delay();

			console.log('=== DEBUG INFO ===');
			console.log('a.length:', a.length);
			console.log('b.length:', b.length);
			console.log('integratedA.length:', integratedA.length);
			console.log('integratedB.length:', integratedB.length);
			console.log('delayedIntegratedB.length:', delayedIntegratedB.length);

			const term1 = integratedA.liftJoin(
				b,
				(x) => x.id,
				(y) => y.id
			);
			const term2 = a.liftJoin(
				delayedIntegratedB,
				(x) => x.id,
				(y) => y.id
			);

			console.log('term1.length:', term1.length);
			console.log('term2.length:', term2.length);

			const expected = term1.plus(term2);
			console.log('expected.length:', expected.length);

			const actual = a.liftJoinIncremental(
				b,
				(x) => x.id,
				(y) => y.id
			);
			console.log('actual.length:', actual.length);

			// Check if lengths match first
			if (expected.length !== actual.length) {
				console.log('LENGTH MISMATCH!');
				console.log('Expected length:', expected.length);
				console.log('Actual length:', actual.length);
			}

			expect(streamsEqual(expected, actual)).toBe(true);
		});

		it('incremental cartesian product works correctly', () => {
			const s1 = new Stream();
			const s2 = new Stream();

			const z1 = new ZSet();
			z1.add('A', 1);
			const z2 = new ZSet();
			z2.add('B', 1);
			s1.append(z1);
			s2.append(z2);

			// Test that incremental cartesian product produces correct results
			const result = s1.liftCartesianProductIncremental(s2);

			// Should produce [['A', 'B']] at time 0
			const expected = new ZSet();
			expected.add(['A', 'B'], 1);
			expect(zsetsEqual(result.get(0), expected)).toBe(true);
		});
	});

	describe('Time-Invariance (Definition 2.6)', () => {
		it('lift operator is time-invariant', () => {
			const s = new Stream();
			const z = new ZSet();
			z.add('test', 1);
			s.append(z);

			const f = (zset: ZSet) => zset.filter((x) => x === 'test');

			// Test S(z^(-1)(s)) = z^(-1)(S(s))
			const left = s.delay().lift(f);
			const right = s.lift(f).delay();

			expect(streamsEqual(left, right)).toBe(true);
		});

		it('differentiation is time-invariant', () => {
			const s = new Stream();
			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 1);
			s.append(z1);
			s.append(z2);

			// Test D(z^(-1)(s)) = z^(-1)(D(s))
			const left = s.delay().differentiate();
			const right = s.differentiate().delay();

			expect(streamsEqual(left, right)).toBe(true);
		});

		it('integration is time-invariant', () => {
			const s = new Stream();
			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 1);
			s.append(z1);
			s.append(z2);

			// Test I(z^(-1)(s)) = z^(-1)(I(s))
			const left = integrate(s.delay());
			const right = integrate(s).delay();

			expect(streamsEqual(left, right)).toBe(true);
		});
	});

	describe('Incremental View Maintenance (Definition 3.1)', () => {
		it('Q^Δ = D ∘ Q ∘ I works correctly', () => {
			const changes = new Stream();
			const change = new ZSet();
			change.add({ name: 'test', value: 42 }, 1);
			changes.append(change);

			const query = (stream: Stream) => stream.liftFilter((item) => item.name === 'test');

			// Apply incremental query
			const result = changes.applyIncremental(query);

			// Should produce the filtered item
			const expected = new ZSet();
			expected.add({ name: 'test', value: 42 }, 1);
			expect(zsetsEqual(result.get(0), expected)).toBe(true);
		});
	});

	describe('Relational Operators (Table 1)', () => {
		it('union operator works correctly', () => {
			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 1);

			const result = z1.union(z2);

			expect(result.materialize.sort()).toEqual(['a', 'b']);
		});

		it('difference operator works correctly', () => {
			const z1 = new ZSet();
			z1.add('a', 1);
			z1.add('b', 1);
			const z2 = new ZSet();
			z2.add('a', 1);

			const result = z1.difference(z2);

			expect(result.materialize).toEqual(['b']);
		});

		it('intersection operator works correctly', () => {
			const z1 = new ZSet();
			z1.add('a', 1);
			z1.add('b', 1);
			const z2 = new ZSet();
			z2.add('a', 1);
			z2.add('c', 1);

			const result = z1.intersection(z2);

			expect(result.materialize).toEqual(['a']);
		});

		it('distinct operator works correctly (Proposition 4.7)', () => {
			const z = new ZSet();
			z.add('item', 3); // weight > 1
			z.add('negative', -1); // negative weight

			const result = z.distinct();

			// Should have weight 1 for positive items, remove negative
			expect(result.materialize).toEqual(['item']);
			expect(result.debug().get('"item"')).toBe(1);
		});
	});
});

// Helper functions
function zsetsEqual(z1: ZSet, z2: ZSet): boolean {
	// Compare using debug() which shows the actual internal state
	const debug1 = z1.debug();
	const debug2 = z2.debug();

	// Check if they have the same number of entries
	if (debug1.size !== debug2.size) return false;

	// Check if all entries match
	for (const [key, weight] of debug1) {
		if (debug2.get(key) !== weight) return false;
	}

	return true;
}

function streamsEqual(s1: Stream, s2: Stream): boolean {
	if (s1.length !== s2.length) return false;
	for (let t = 0; t < s1.length; t++) {
		if (!zsetsEqual(s1.get(t), s2.get(t))) {
			console.log(`Streams differ at time ${t}:`);
			console.log('s1[t]:', s1.get(t).debug());
			console.log('s2[t]:', s2.get(t).debug());
			return false;
		}
	}
	return true;
}

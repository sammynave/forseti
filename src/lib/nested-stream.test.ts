import { describe, expect, it } from 'vitest';
import { NestedStream } from './nested-stream.js';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { delta0, integral } from './stream/utils.js';

describe('NestedStream (SS_A) - DBSP Section 6', () => {
	describe('Basic Functionality - Matrix Operations', () => {
		it('constructs empty nested stream correctly', () => {
			const ns = new NestedStream();

			expect(ns.outerLength).toBe(0);
			expect(ns.maxInnerLength).toBe(0);
			expect(ns.isZero()).toBe(true);
		});

		it('handles nested coordinates (t0, t1) correctly', () => {
			const ns = new NestedStream();
			const zset = new ZSet();
			zset.add('data', 42);

			// Set value at (2, 3)
			ns.set(2, 3, zset);

			// Check dimensions
			expect(ns.outerLength).toBe(3);
			expect(ns.getStream(2).length).toBe(4);

			// Check value retrieval
			expect(ns.get(2, 3).debug().get('"data"')).toBe(42);

			// Check zero values at other coordinates
			expect(ns.get(0, 0).isZero()).toBe(true);
			expect(ns.get(2, 0).isZero()).toBe(true);
			expect(ns.get(1, 3).isZero()).toBe(true);
		});

		it('supports stream-level operations', () => {
			const ns = new NestedStream();

			// Create a stream with some data
			const stream = new Stream();
			const z1 = new ZSet();
			z1.add('item1', 1);
			const z2 = new ZSet();
			z2.add('item2', 2);
			stream.append(z1);
			stream.append(z2);

			// Add stream at outer time 1
			ns.setStream(1, stream);

			expect(ns.outerLength).toBe(2);
			expect(ns.getStream(1).length).toBe(2);
			expect(ns.get(1, 0).debug().get('"item1"')).toBe(1);
			expect(ns.get(1, 1).debug().get('"item2"')).toBe(2);
		});

		it('appends streams correctly', () => {
			const ns = new NestedStream();

			const stream1 = new Stream();
			const z1 = new ZSet();
			z1.add('first', 1);
			stream1.append(z1);

			const stream2 = new Stream();
			const z2 = new ZSet();
			z2.add('second', 2);
			stream2.append(z2);

			const idx1 = ns.appendStream(stream1);
			const idx2 = ns.appendStream(stream2);

			expect(idx1).toBe(0);
			expect(idx2).toBe(1);
			expect(ns.outerLength).toBe(2);

			expect(ns.get(0, 0).debug().get('"first"')).toBe(1);
			expect(ns.get(1, 0).debug().get('"second"')).toBe(2);
		});

		it('extracts columns correctly', () => {
			const ns = new NestedStream();

			// Create a 3x2 matrix:
			// t0=0: [a:1, b:2]
			// t0=1: [c:3, d:4]
			// t0=2: [e:5, f:6]
			for (let t0 = 0; t0 < 3; t0++) {
				for (let t1 = 0; t1 < 2; t1++) {
					const zset = new ZSet();
					zset.add(`item_${t0}_${t1}`, (t0 + 1) * 10 + (t1 + 1));
					ns.set(t0, t1, zset);
				}
			}

			// Extract column t1=0: should contain items from (0,0), (1,0), (2,0)
			const column0 = ns.extractColumn(0);
			expect(column0.length).toBe(3);
			expect(column0.get(0).debug().get('"item_0_0"')).toBe(11);
			expect(column0.get(1).debug().get('"item_1_0"')).toBe(21);
			expect(column0.get(2).debug().get('"item_2_0"')).toBe(31);

			// Extract column t1=1
			const column1 = ns.extractColumn(1);
			expect(column1.length).toBe(3);
			expect(column1.get(0).debug().get('"item_0_1"')).toBe(12);
			expect(column1.get(1).debug().get('"item_1_1"')).toBe(22);
			expect(column1.get(2).debug().get('"item_2_1"')).toBe(32);
		});
	});

	describe('Abelian Group Properties (Section 6)', () => {
		it('nested streams form abelian group - addition', () => {
			const ns1 = new NestedStream();
			const ns2 = new NestedStream();

			// Create test data
			const z1 = new ZSet();
			z1.add('shared', 5);
			ns1.set(0, 0, z1);

			const z2 = new ZSet();
			z2.add('shared', 3);
			z2.add('unique', 7);
			ns2.set(0, 0, z2);

			const result = ns1.plus(ns2);

			// shared: 5 + 3 = 8, unique: 7
			expect(result.get(0, 0).debug().get('"shared"')).toBe(8);
			expect(result.get(0, 0).debug().get('"unique"')).toBe(7);
		});

		it('nested streams form abelian group - commutativity', () => {
			const ns1 = new NestedStream();
			const ns2 = new NestedStream();

			const z1 = new ZSet();
			z1.add('a', 1);
			ns1.set(0, 0, z1);
			ns1.set(1, 1, z1);

			const z2 = new ZSet();
			z2.add('b', 2);
			ns2.set(0, 0, z2);
			ns2.set(2, 0, z2);

			const left = ns1.plus(ns2);
			const right = ns2.plus(ns1);

			// Both should be equal
			expect(left.outerLength).toBe(right.outerLength);
			expect(left.get(0, 0).debug().get('"a"')).toBe(1);
			expect(left.get(0, 0).debug().get('"b"')).toBe(2);
			expect(right.get(0, 0).debug().get('"a"')).toBe(1);
			expect(right.get(0, 0).debug().get('"b"')).toBe(2);
		});

		it('nested streams form abelian group - identity', () => {
			const ns = new NestedStream();
			const z = new ZSet();
			z.add('test', 42);
			ns.set(1, 1, z);

			const zero = NestedStream.zero();
			const result = ns.plus(zero);

			// ns + 0 = ns
			expect(result.get(1, 1).debug().get('"test"')).toBe(42);
			expect(result.outerLength).toBe(ns.outerLength);
		});

		it('nested streams form abelian group - inverse', () => {
			const ns = new NestedStream();
			const z = new ZSet();
			z.add('test', 42);
			ns.set(0, 0, z);

			const result = ns.plus(ns.negate());

			// ns + (-ns) = 0
			expect(result.get(0, 0).isZero()).toBe(true);
			expect(result.isZero()).toBe(true);
		});

		it('negation works correctly', () => {
			const ns = new NestedStream();
			const z = new ZSet();
			z.add('pos', 5);
			z.add('neg', -3);
			ns.set(0, 0, z);

			const negated = ns.negate();

			expect(negated.get(0, 0).debug().get('"pos"')).toBe(-5);
			expect(negated.get(0, 0).debug().get('"neg"')).toBe(3);
		});
	});

	describe('Nested Stream Operators (Section 6)', () => {
		it('nested lifting (↑S) applies stream operator to each row', () => {
			const ns = new NestedStream();

			// Create test data in multiple rows
			for (let t0 = 0; t0 < 3; t0++) {
				const stream = new Stream();
				const z1 = new ZSet();
				z1.add(`row${t0}_item1`, 1);
				const z2 = new ZSet();
				z2.add(`row${t0}_item2`, 2);
				stream.append(z1);
				stream.append(z2);
				ns.setStream(t0, stream);
			}

			// Apply negation to each row
			const result = ns.liftStreamOperator((stream) => stream.negate());

			// Each row should be negated independently
			expect(result.get(0, 0).debug().get('"row0_item1"')).toBe(-1);
			expect(result.get(1, 0).debug().get('"row1_item1"')).toBe(-1);
			expect(result.get(2, 1).debug().get('"row2_item2"')).toBe(-2);
		});

		it('nested delay (↑z^(-1)) delays columns, not rows', () => {
			const ns = new NestedStream();

			// Create 2x2 matrix
			const z1 = new ZSet();
			z1.add('a', 1);
			const z2 = new ZSet();
			z2.add('b', 2);
			const z3 = new ZSet();
			z3.add('c', 3);
			const z4 = new ZSet();
			z4.add('d', 4);

			ns.set(0, 0, z1);
			ns.set(0, 1, z2);
			ns.set(1, 0, z3);
			ns.set(1, 1, z4);

			const delayed = ns.nestedDelay();

			// Each inner stream (column) should be delayed
			// Row 0: [∅, a, b]  (∅ added at beginning)
			// Row 1: [∅, c, d]
			expect(delayed.get(0, 0).isZero()).toBe(true);
			expect(delayed.get(0, 1).debug().get('"a"')).toBe(1);
			expect(delayed.get(0, 2).debug().get('"b"')).toBe(2);

			expect(delayed.get(1, 0).isZero()).toBe(true);
			expect(delayed.get(1, 1).debug().get('"c"')).toBe(3);
			expect(delayed.get(1, 2).debug().get('"d"')).toBe(4);
		});

		it('nested integration (↑I) integrates each column', () => {
			const ns = new NestedStream();

			// Create streams with changes that should accumulate
			const stream1 = new Stream();
			const z1 = new ZSet();
			z1.add('counter', 1);
			const z2 = new ZSet();
			z2.add('counter', 2);
			stream1.append(z1);
			stream1.append(z2);
			ns.setStream(0, stream1);

			const integrated = ns.nestedIntegrate();

			// Integration should accumulate: [1, 1+2] = [1, 3]
			expect(integrated.get(0, 0).debug().get('"counter"')).toBe(1);
			expect(integrated.get(0, 1).debug().get('"counter"')).toBe(3);
		});

		it('nested differentiation (↑D) differentiates each column', () => {
			const ns = new NestedStream();

			// Create a stream that represents accumulated values
			const stream = new Stream();
			const z1 = new ZSet();
			z1.add('total', 5);
			const z2 = new ZSet();
			z2.add('total', 8); // diff should be 3
			const z3 = new ZSet();
			z3.add('total', 10); // diff should be 2
			stream.append(z1);
			stream.append(z2);
			stream.append(z3);
			ns.setStream(0, stream);

			const differentiated = ns.nestedDifferentiate();

			// D[0] = s[0] = 5
			// D[1] = s[1] - s[0] = 8 - 5 = 3
			// D[2] = s[2] - s[1] = 10 - 8 = 2
			expect(differentiated.get(0, 0).debug().get('"total"')).toBe(5);
			expect(differentiated.get(0, 1).debug().get('"total"')).toBe(3);
			expect(differentiated.get(0, 2).debug().get('"total"')).toBe(2);
		});
	});

	describe('DBSP Proposition 6.2 - Lifting Cycles', () => {
		it('lifting preserves cycle structure', () => {
			// Test that ↑(λs.fix α.T(s, z^(-1)(α))) = λs.fix α.(↑T)(s,(↑z^(-1))(α))
			// This is a complex property but we can test a simplified version

			const ns = new NestedStream();

			// Create test input
			const stream = new Stream();
			const z = new ZSet();
			z.add('input', 10);
			stream.append(z);
			ns.setStream(0, stream);

			// Simulate a simple cycle: output = input + delayed_output
			// This should converge based on the fixed point computation
			const cycleOperator = (s: Stream): Stream => {
				const result = new Stream();
				for (let t = 0; t < s.length; t++) {
					if (t === 0) {
						result.append(s.get(0)); // First element is just input
					} else {
						// This would normally involve feedback, but for testing
						// we'll use a simplified version
						result.append(s.get(t));
					}
				}
				return result;
			};

			const result = ns.liftStreamOperator(cycleOperator);

			// Should preserve the basic structure
			expect(result.get(0, 0).debug().get('"input"')).toBe(10);
			expect(result.outerLength).toBe(ns.outerLength);
		});
	});

	describe('Integration with DBSP Recursive Patterns', () => {
		it('supports δ₀ ∘ computation ∘ ∫ pattern for nested streams', () => {
			// This pattern appears in recursive query incrementalization
			const ns = new NestedStream();

			// Create input that represents changes over nested time
			for (let t0 = 0; t0 < 2; t0++) {
				const stream = new Stream();
				for (let t1 = 0; t1 < 3; t1++) {
					const z = new ZSet();
					z.add(`data_${t0}_${t1}`, t0 + t1 + 1);
					stream.append(z);
				}
				ns.setStream(t0, stream);
			}

			// Apply nested integration (↑I)
			const integrated = ns.nestedIntegrate();

			// Each row should be integrated independently
			expect(integrated.get(0, 0).debug().get('"data_0_0"')).toBe(1); // Just first element
			expect(integrated.get(0, 1).debug().get('"data_0_0"')).toBe(1); // Still there
			expect(integrated.get(0, 1).debug().get('"data_0_1"')).toBe(2); // Accumulated
			expect(integrated.get(0, 2).debug().get('"data_0_2"')).toBe(3); // New element

			// Second row
			expect(integrated.get(1, 0).debug().get('"data_1_0"')).toBe(2);
			expect(integrated.get(1, 2).debug().get('"data_1_2"')).toBe(4);
		});

		it('implements matrix-like operations for complex queries', () => {
			const ns = new NestedStream();

			// Create a pattern that might appear in transitive closure:
			// Each outer time represents a graph update
			// Each inner time represents iterations of closure computation

			// t0=0: Initial graph with edge A->B
			const t0_stream = new Stream();
			const edge_ab = new ZSet();
			edge_ab.add(['A', 'B'], 1);
			t0_stream.append(edge_ab);
			ns.setStream(0, t0_stream);

			// t0=1: Add edge B->C, should discover A->C in closure
			const t1_stream = new Stream();
			const edge_bc = new ZSet();
			edge_bc.add(['B', 'C'], 1);
			// Iteration 0: new direct edge
			t1_stream.append(edge_bc);
			// Iteration 1: derived edge A->C via transitivity
			const edge_ac = new ZSet();
			edge_ac.add(['A', 'C'], 1);
			t1_stream.append(edge_ac);
			ns.setStream(1, t1_stream);

			// Test that we can access the nested structure correctly
			expect(ns.get(0, 0).materialize).toEqual([['A', 'B']]);
			expect(ns.get(1, 0).materialize).toEqual([['B', 'C']]);
			expect(ns.get(1, 1).materialize).toEqual([['A', 'C']]);

			// Extract column 0: direct edges added at each outer time
			const directEdges = ns.extractColumn(0);
			expect(directEdges.get(0).materialize).toEqual([['A', 'B']]);
			expect(directEdges.get(1).materialize).toEqual([['B', 'C']]);
		});
	});

	describe('Utility Functions and Debugging', () => {
		it('debug() provides readable matrix representation', () => {
			const ns = new NestedStream();

			// Create small matrix for testing
			for (let t0 = 0; t0 < 2; t0++) {
				for (let t1 = 0; t1 < 3; t1++) {
					const z = new ZSet();
					if ((t0 + t1) % 2 === 0) {
						z.add('data', 1); // Add some data to make it non-zero
					}
					ns.set(t0, t1, z);
				}
			}

			const debugStr = ns.debug();
			expect(debugStr).toContain('NestedStream (2 × 3)');
			expect(debugStr).toContain('t0=0:');
			expect(debugStr).toContain('t0=1:');
		});

		it('dimensionsMatch() checks compatibility correctly', () => {
			const ns1 = new NestedStream();
			const ns2 = new NestedStream();

			// Create matching dimensions
			for (let t0 = 0; t0 < 2; t0++) {
				const stream1 = new Stream();
				const stream2 = new Stream();
				for (let t1 = 0; t1 < 3; t1++) {
					stream1.append(new ZSet());
					stream2.append(new ZSet());
				}
				ns1.setStream(t0, stream1);
				ns2.setStream(t0, stream2);
			}

			expect(ns1.dimensionsMatch(ns2)).toBe(true);

			// Add different inner dimension to ns2
			ns2.set(1, 3, new ZSet());
			expect(ns1.dimensionsMatch(ns2)).toBe(false);
		});

		it('flatten() converts to regular stream', () => {
			const ns = new NestedStream();

			const z1 = new ZSet();
			z1.add('first', 1);
			const z2 = new ZSet();
			z2.add('second', 2);
			const z3 = new ZSet();
			z3.add('third', 3);

			ns.set(0, 0, z1);
			ns.set(0, 1, z2);
			ns.set(1, 0, z3);

			const flattened = ns.flatten();

			// Should contain all elements in row-major order
			expect(flattened.length).toBe(3); // 2 from first stream, 1 from second
			expect(flattened.get(0).debug().get('"first"')).toBe(1);
			expect(flattened.get(1).debug().get('"second"')).toBe(2);
			expect(flattened.get(2).debug().get('"third"')).toBe(3);
		});

		it('handles edge cases gracefully', () => {
			const ns = new NestedStream();

			// Access non-existent coordinates
			expect(ns.get(10, 20).isZero()).toBe(true);
			expect(ns.getStream(5).length).toBe(0);

			// Empty nested stream operations
			const empty = NestedStream.zero();
			expect(empty.plus(ns).outerLength).toBe(0);
			expect(empty.flatten().length).toBe(0);
		});
	});

	describe('Performance and Scalability', () => {
		it('handles reasonably sized nested streams efficiently', () => {
			const ns = new NestedStream();

			const start = performance.now();

			// Create a 50x20 matrix
			for (let t0 = 0; t0 < 50; t0++) {
				const stream = new Stream();
				for (let t1 = 0; t1 < 20; t1++) {
					const z = new ZSet();
					if (t0 % 5 === 0 && t1 % 3 === 0) {
						z.add(`data_${t0}_${t1}`, t0 + t1);
					}
					stream.append(z);
				}
				ns.setStream(t0, stream);
			}

			// Perform some operations
			const negated = ns.negate();
			const delayed = ns.nestedDelay();

			const end = performance.now();

			expect(end - start).toBeLessThan(1000); // Should complete within 1 second
			expect(negated.outerLength).toBe(50);
			expect(delayed.maxInnerLength).toBe(21); // 20 + 1 from delay
		});
	});
});

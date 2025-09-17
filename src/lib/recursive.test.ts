import { describe, expect, it } from 'vitest';
import {
	computeFixedPoint,
	semiNaiveFixedPoint,
	streamingFixedPoint,
	transitiveClosure,
	incrementalTransitiveClosure,
	graphReachability,
	createGraphFromAdjacencyList,
	extractNodes,
	type RecursiveResult
} from './recursive.js';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { delta0, integral } from './stream/utils.js';

describe('Recursive Query Infrastructure (DBSP Section 5)', () => {
	describe('Fixed-Point Computation', () => {
		it('computes simple fixed point correctly', () => {
			const initial = new ZSet();
			initial.add('start', 1);

			// Fixed point: result = input (identity function)
			const result = computeFixedPoint(
				initial,
				(current) => current, // Identity operation
				{ maxIterations: 10 }
			);

			expect(result.converged).toBe(true);
			expect(result.iterations).toBe(1); // Should converge immediately
			expect(result.result.materialize).toEqual(['start']);
		});

		it('handles growing fixed point', () => {
			const initial = new ZSet();
			initial.add('seed', 1);

			// Each iteration adds a new element (until we hit max)
			let counter = 0;
			const result = computeFixedPoint(
				initial,
				(current) => {
					counter++;
					const next = new ZSet();
					// Copy current elements
					for (const [key, weight] of current.debug()) {
						next.add(JSON.parse(key), weight);
					}
					// Add new element (but stop growing after 3 iterations)
					if (counter <= 3) {
						next.add(`item_${counter}`, 1);
					}
					return next;
				},
				{ maxIterations: 10 }
			);

			expect(result.converged).toBe(true);
			expect(result.result.materialize.length).toBe(4); // seed + 3 new items
			expect(result.result.materialize).toContain('seed');
			expect(result.result.materialize).toContain('item_1');
			expect(result.result.materialize).toContain('item_3');
		});

		it('respects maximum iterations', () => {
			const initial = new ZSet();
			initial.add('infinite', 1);

			// Infinite growth (never converges)
			let counter = 0;
			const result = computeFixedPoint(
				initial,
				(current) => {
					counter++;
					const next = new ZSet();
					for (const [key, weight] of current.debug()) {
						next.add(JSON.parse(key), weight);
					}
					next.add(`infinite_${counter}`, 1); // Always add new element
					return next;
				},
				{ maxIterations: 5 }
			);

			expect(result.converged).toBe(false);
			expect(result.iterations).toBe(5);
		});

		it('uses custom termination condition', () => {
			const initial = new ZSet();
			initial.add('counter', 1);

			const result = computeFixedPoint(
				initial,
				(current) => {
					const next = new ZSet();
					const currentCount = current.debug().get('"counter"') || 0;
					next.add('counter', currentCount + 1);
					return next;
				},
				{
					maxIterations: 100,
					terminationCondition: (iteration, current, previous) => {
						return current.debug().get('"counter"') >= 10; // Stop at 10
					}
				}
			);

			expect(result.converged).toBe(false); // Early termination
			expect(result.result.debug().get('"counter"') ?? 0).toBe(10);
		});

		it('keeps trace when requested', () => {
			const initial = new ZSet();
			initial.add('trace', 1);

			const result = computeFixedPoint(
				initial,
				(current) => {
					const count = current.debug().get('"trace"') || 0;
					if (count >= 3) return current; // Converge at 3
					const next = new ZSet();
					next.add('trace', count + 1);
					return next;
				},
				{ keepTrace: true }
			);

			expect(result.trace.length).toBe(4); // Initial + 3 iterations
			expect(result.trace.get(0).debug().get('"trace"')).toBe(1);
			expect(result.trace.get(1).debug().get('"trace"')).toBe(2);
			expect(result.trace.get(2).debug().get('"trace"')).toBe(3);
			expect(result.trace.get(3).debug().get('"trace"')).toBe(3); // Final
		});
	});

	describe('Semi-Naïve Fixed-Point Computation', () => {
		it('optimizes computation using deltas', () => {
			const initial = new ZSet();
			initial.add('a', 1);

			let totalOperations = 0;

			const result = semiNaiveFixedPoint(
				initial,
				(accumulated, delta) => {
					totalOperations++;
					const newFacts = new ZSet();

					// Only process elements in delta (semi-naïve optimization)
					for (const [key, weight] of delta.debug()) {
						if (weight > 0) {
							const item = JSON.parse(key);
							// Simple rule: if we have 'a', derive 'b'; if 'b', derive 'c'
							if (item === 'a') {
								newFacts.add('b', weight);
							} else if (item === 'b') {
								newFacts.add('c', weight);
							}
						}
					}

					return newFacts;
				},
				{ maxIterations: 10 }
			);

			expect(result.converged).toBe(true);
			expect(result.result.materialize.sort()).toEqual(['a', 'b', 'c']);
			// Semi-naïve should be more efficient than naïve
			expect(totalOperations).toBe(3); // Three iterations: a->b, b->c, (convergence detected, do nothing)
		});

		it('handles complex accumulation correctly', () => {
			const initial = new ZSet();
			initial.add(['root', 'child1'], 1);

			const result = semiNaiveFixedPoint(initial, (accumulated, delta) => {
				const newFacts = new ZSet();

				// Rule: if parent->child, then grandparent->grandchild
				for (const [deltaKey, deltaWeight] of delta.debug()) {
					if (deltaWeight > 0) {
						const [parent, child] = JSON.parse(deltaKey);

						// Look for existing parent relationships
						for (const [accKey, accWeight] of accumulated.debug()) {
							if (accWeight > 0) {
								const [grandparent, maybeParent] = JSON.parse(accKey);
								if (maybeParent === parent) {
									newFacts.add([grandparent, child], deltaWeight * accWeight);
								}
							}
						}
					}
				}

				return newFacts;
			});

			expect(result.converged).toBe(true);
			// Should only have the initial relationship (no valid derivations)
			expect(result.result.materialize).toEqual([['root', 'child1']]);
		});

		it('detects convergence when no new facts derived', () => {
			const initial = new ZSet();
			initial.add('terminal', 1);

			const result = semiNaiveFixedPoint(initial, (accumulated, delta) => {
				// Never derive any new facts
				return new ZSet();
			});

			expect(result.converged).toBe(true);
			expect(result.iterations).toBe(1);
			expect(result.result.materialize).toEqual(['terminal']);
		});
	});

	describe('Streaming Fixed-Point', () => {
		it('applies fixed point to stream elements', () => {
			const inputStream = new Stream();

			const z1 = new ZSet();
			z1.add('input1', 1);
			const z2 = new ZSet();
			z2.add('input2', 1);

			inputStream.append(z1);
			inputStream.append(z2);

			const result = streamingFixedPoint(inputStream, (input, current) => {
				// Simple doubling rule
				const doubled = new ZSet();
				for (const [key, weight] of input.debug()) {
					doubled.add(JSON.parse(key), weight * 2);
				}
				return doubled;
			});

			expect(result.length).toBe(2);
			expect(result.get(0).debug().get('"input1"')).toBe(2);
			expect(result.get(1).debug().get('"input2"')).toBe(2);
		});
	});

	describe('Transitive Closure', () => {
		it('computes simple transitive closure', () => {
			const edges = new ZSet();
			edges.add(['A', 'B'], 1);
			edges.add(['B', 'C'], 1);
			edges.add(['C', 'D'], 1);

			const result = transitiveClosure(edges, { semiNaive: true });

			expect(result.converged).toBe(true);

			const paths = result.result.materialize;
			// Should include all direct and derived paths
			expect(paths).toContainEqual(['A', 'B']);
			expect(paths).toContainEqual(['B', 'C']);
			expect(paths).toContainEqual(['C', 'D']);
			expect(paths).toContainEqual(['A', 'C']); // A->B->C
			expect(paths).toContainEqual(['B', 'D']); // B->C->D
			expect(paths).toContainEqual(['A', 'D']); // A->B->C->D
		});

		it('handles cycles correctly', () => {
			const edges = new ZSet();
			edges.add(['A', 'B'], 1);
			edges.add(['B', 'A'], 1); // Cycle

			const result = transitiveClosure(edges);

			expect(result.converged).toBe(true);
			const paths = result.result.materialize;

			// Should include both directions
			expect(paths).toContainEqual(['A', 'B']);
			expect(paths).toContainEqual(['B', 'A']);
			// Self-loops should be derived: A->B->A = A->A
			expect(paths).toContainEqual(['A', 'A']);
			expect(paths).toContainEqual(['B', 'B']);
		});

		it('compares semi-naive vs naive evaluation', () => {
			const edges = createGraphFromAdjacencyList({
				A: ['B'],
				B: ['C', 'D'],
				C: ['E'],
				D: ['E']
			});

			const semiNaiveResult = transitiveClosure(edges, { semiNaive: true });
			const naiveResult = transitiveClosure(edges, { semiNaive: false });

			// Results should be the same
			expect(semiNaiveResult.converged).toBe(true);
			expect(naiveResult.converged).toBe(true);

			const semiNaivePaths = new Set(
				semiNaiveResult.result.materialize.map((p) => JSON.stringify(p))
			);
			const naivePaths = new Set(naiveResult.result.materialize.map((p) => JSON.stringify(p)));

			expect(semiNaivePaths).toEqual(naivePaths);

			// Semi-naive should be more efficient (fewer iterations)
			expect(semiNaiveResult.iterations).toBeLessThanOrEqual(naiveResult.iterations);
		});

		it('works with weighted edges', () => {
			const edges = new ZSet();
			edges.add(['A', 'B'], 2); // Weight 2
			edges.add(['B', 'C'], 3); // Weight 3

			const result = transitiveClosure(edges);

			// A->C should have weight 2*3 = 6
			const acWeight = result.result.debug().get('["A","C"]');
			expect(acWeight).toBe(6);
		});
	});

	describe('Incremental Transitive Closure', () => {
		it('maintains closure incrementally', () => {
			const changes = new Stream();

			// t=0: Add initial edges
			const initial = new ZSet();
			initial.add(['A', 'B'], 1);
			initial.add(['B', 'C'], 1);
			changes.append(initial);

			// t=1: Add new edge that creates more paths
			const newEdge = new ZSet();
			newEdge.add(['C', 'D'], 1);
			changes.append(newEdge);

			const result = incrementalTransitiveClosure(changes);

			expect(result.length).toBe(2);

			// At t=0: A->B, B->C, A->C
			const closure0 = result.get(0).materialize;
			expect(closure0).toContainEqual(['A', 'B']);
			expect(closure0).toContainEqual(['B', 'C']);
			expect(closure0).toContainEqual(['A', 'C']);

			// At t=1: Previous + C->D, A->D, B->D
			const closure1 = result.get(1).materialize;
			expect(closure1).toContainEqual(['C', 'D']);
			expect(closure1).toContainEqual(['A', 'D']); // New derived path
			expect(closure1).toContainEqual(['B', 'D']); // New derived path
		});

		it('handles edge removal', () => {
			const changes = new Stream();

			// Add edges
			const add = new ZSet();
			add.add(['A', 'B'], 1);
			add.add(['B', 'C'], 1);
			changes.append(add);

			// Remove an edge
			const remove = new ZSet();
			remove.add(['A', 'B'], -1); // Remove A->B
			changes.append(remove);

			const result = incrementalTransitiveClosure(changes);

			// After removal, A->C should no longer be reachable
			const finalClosure = result.get(1).materialize;
			expect(finalClosure).not.toContainEqual(['A', 'C']);
			expect(finalClosure).toContainEqual(['B', 'C']); // This should remain
		});
	});

	describe('Graph Reachability', () => {
		it('finds all reachable nodes from start set', () => {
			const edges = createGraphFromAdjacencyList({
				A: ['B', 'C'],
				B: ['D'],
				C: ['E'],
				D: ['F'],
				E: ['F'],
				X: ['Y'] // Disconnected component
			});

			const startNodes = new ZSet();
			startNodes.add('A', 1);

			const result = graphReachability(edges, startNodes);

			expect(result.converged).toBe(true);

			const reachableNodes = extractNodes(result.result);
			expect(reachableNodes).toContain('A'); // Start node
			expect(reachableNodes).toContain('B');
			expect(reachableNodes).toContain('C');
			expect(reachableNodes).toContain('D');
			expect(reachableNodes).toContain('E');
			expect(reachableNodes).toContain('F');

			// Should not reach disconnected component
			expect(reachableNodes).not.toContain('X');
			expect(reachableNodes).not.toContain('Y');
		});

		it('handles multiple start nodes', () => {
			const edges = createGraphFromAdjacencyList({
				A: ['B'],
				C: ['D'],
				B: ['X'],
				D: ['X']
			});

			const startNodes = new ZSet();
			startNodes.add('A', 1);
			startNodes.add('C', 1);

			const result = graphReachability(edges, startNodes);

			const reachableNodes = extractNodes(result.result);
			expect(reachableNodes).toContain('A');
			expect(reachableNodes).toContain('B');
			expect(reachableNodes).toContain('C');
			expect(reachableNodes).toContain('D');
			expect(reachableNodes).toContain('X'); // Reachable from both paths
		});

		it('handles empty graph', () => {
			const edges = new ZSet(); // No edges
			const startNodes = new ZSet();
			startNodes.add('isolated', 1);

			const result = graphReachability(edges, startNodes);

			expect(result.converged).toBe(true);
			const reachableNodes = extractNodes(result.result);
			expect(reachableNodes.size).toBe(1);
			expect(reachableNodes).toContain('isolated');
		});
	});

	describe('Utility Functions', () => {
		it('createGraphFromAdjacencyList works correctly', () => {
			const adj = {
				A: ['B', 'C'],
				B: ['C'],
				D: []
			};

			const edges = createGraphFromAdjacencyList(adj);

			expect(edges.materialize).toContainEqual(['A', 'B']);
			expect(edges.materialize).toContainEqual(['A', 'C']);
			expect(edges.materialize).toContainEqual(['B', 'C']);
			expect(edges.materialize).toHaveLength(3);
		});

		it('extractNodes extracts node names correctly', () => {
			const zset = new ZSet();
			zset.add('node1', 1);
			zset.add('node2', 2);
			zset.add('node3', -1); // Negative weight should be ignored

			const nodes = extractNodes(zset);

			expect(nodes.size).toBe(2);
			expect(nodes).toContain('node1');
			expect(nodes).toContain('node2');
			expect(nodes).not.toContain('node3');
		});
	});

	describe('DBSP Property Compliance', () => {
		it('fixed-point operator satisfies uniqueness (Proposition 2.9)', () => {
			const initial = new ZSet();
			initial.add('test', 5);

			// Same operation should always give same result
			const op = (current: ZSet): ZSet => {
				const weight = current.debug().get('"test"') || 0;
				if (weight >= 10) return current; // Stop at 10
				const next = new ZSet();
				next.add('test', Math.min(weight + 1, 10));
				return next;
			};

			const result1 = computeFixedPoint(initial, op);
			const result2 = computeFixedPoint(initial, op);

			// Results should be identical
			expect(result1.result.debug().get('"test"')).toBe(result2.result.debug().get('"test"'));
			expect(result1.converged).toBe(result2.converged);
			expect(result1.iterations).toBe(result2.iterations);
		});

		it('semi-naïve evaluation is equivalent to naïve', () => {
			// Test the equivalence property mentioned in the paper
			const initial = new ZSet();
			initial.add(['A', 'B'], 1);
			initial.add(['B', 'C'], 1);

			// Naïve evaluation (recompute everything)
			const naiveResult = computeFixedPoint(initial, (current) => {
				// Transitive closure: if X->Y and Y->Z then X->Z
				const newPaths = new ZSet();

				// Include original edges
				for (const [key, weight] of initial.debug()) {
					newPaths.add(JSON.parse(key), weight);
				}

				// Add derived paths
				for (const [key1, weight1] of current.debug()) {
					if (weight1 > 0) {
						const [start1, end1] = JSON.parse(key1);
						for (const [key2, weight2] of current.debug()) {
							if (weight2 > 0) {
								const [start2, end2] = JSON.parse(key2);
								if (end1 === start2) {
									newPaths.add([start1, end2], weight1 * weight2);
								}
							}
						}
					}
				}
				return newPaths.distinct();
			});

			// Semi-naïve evaluation
			const semiNaiveResult = semiNaiveFixedPoint(initial, (accumulated, delta) => {
				const newPaths = new ZSet();

				// Join accumulated paths with original edges to find new paths
				for (const [deltaKey, deltaWeight] of delta.debug()) {
					if (deltaWeight > 0) {
						const [deltaStart, deltaEnd] = JSON.parse(deltaKey);

						// Extend with edges
						for (const [edgeKey, edgeWeight] of initial.debug()) {
							if (edgeWeight > 0) {
								const [edgeStart, edgeEnd] = JSON.parse(edgeKey);
								if (deltaEnd === edgeStart) {
									newPaths.add([deltaStart, edgeEnd], deltaWeight * edgeWeight);
								}
							}
						}
					}
				}
				return newPaths;
			});

			// Results should be equivalent
			const naivePaths = new Set(naiveResult.result.materialize.map((p) => JSON.stringify(p)));
			const semiNaivePaths = new Set(
				semiNaiveResult.result.materialize.map((p) => JSON.stringify(p))
			);

			expect(naivePaths).toEqual(semiNaivePaths);
		});
	});

	describe('Performance and Edge Cases', () => {
		it('handles large graphs efficiently', () => {
			// Create a larger graph for performance testing
			const adj: Record<string, string[]> = {};
			for (let i = 0; i < 20; i++) {
				adj[`node${i}`] = [`node${(i + 1) % 20}`]; // Circular graph
			}

			const edges = createGraphFromAdjacencyList(adj);

			const start = performance.now();
			const result = transitiveClosure(edges, { semiNaive: true });
			const end = performance.now();

			expect(result.converged).toBe(true);
			expect(end - start).toBeLessThan(1000); // Should complete in reasonable time

			// In a circular graph, every node should be reachable from every other node
			expect(result.result.materialize).toHaveLength(20 * 20); // n² paths
		});

		it('handles empty inputs gracefully', () => {
			const empty = new ZSet();

			const fixedPointResult = computeFixedPoint(empty, (x) => x);
			expect(fixedPointResult.result.isZero()).toBe(true);

			const closureResult = transitiveClosure(empty);
			expect(closureResult.result.isZero()).toBe(true);

			const reachabilityResult = graphReachability(empty, empty);
			expect(reachabilityResult.result.isZero()).toBe(true);
		});

		it('terminates on self-referential operations', () => {
			const initial = new ZSet();
			initial.add('self', 1);

			const result = computeFixedPoint(initial, (current) => {
				// Operation that references itself should still converge
				return current; // Identity = immediate convergence
			});

			expect(result.converged).toBe(true);
			expect(result.iterations).toBe(1);
		});
	});
});

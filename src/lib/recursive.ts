import { Stream } from './stream.js';
import { NestedStream } from './nested-stream.js';
import { ZSet } from './z-set.js';
import { delta0, integral, integrate } from './stream/utils.js';

/**
 * Recursive Query Infrastructure for DBSP
 *
 * Paper Reference: Section 5, Algorithm 5.3
 *
 * This module implements the infrastructure needed for recursive queries like:
 * - Transitive closure
 * - Graph reachability
 * - SQL recursive CTEs
 * - Any fixed-point computation
 *
 * KEY CONCEPTS:
 * 1. Recursive queries are modeled as: O = R(I, O) where O appears on both sides
 * 2. Fixed-point computation using the pattern: δ₀ ∘ (iterative computation) ∘ ∫
 * 3. Semi-naïve evaluation for efficiency
 * 4. Proper termination detection
 *
 * REAL-WORLD EXAMPLE: LinkedIn Connection Recommendations
 *
 * LinkedIn wants to suggest "people you may know" based on mutual connections.
 * This is essentially computing the transitive closure of the connection graph:
 *
 * - Direct connections: Alice -> Bob, Bob -> Charlie
 * - Derived connections: Alice -> Charlie (via Bob)
 * - When new connections are added, update recommendations incrementally
 *
 * Traditional approach:
 * - Recompute entire graph traversal on every connection update
 * - Expensive for millions of users
 *
 * DBSP recursive approach:
 * - Maintain transitive closure incrementally
 * - Only process changes, not entire graph
 * - Converges quickly due to semi-naïve evaluation
 */

/**
 * Recursive Query Result
 * Contains both the final result and metadata about the computation
 */
export interface RecursiveResult {
	/** The final result of the recursive computation */
	result: ZSet;

	/** Number of iterations until convergence */
	iterations: number;

	/** Whether the computation converged (reached fixed point) */
	converged: boolean;

	/** Stream of intermediate results during fixed-point computation */
	trace: Stream;
}

/**
 * Options for recursive query execution
 */
export interface RecursiveOptions {
	/** Maximum number of iterations before giving up */
	maxIterations?: number;

	/** Custom termination condition */
	terminationCondition?: (iteration: number, current: ZSet, previous: ZSet) => boolean;

	/** Whether to keep trace of intermediate results */
	keepTrace?: boolean;

	/** Whether to use semi-naïve evaluation optimization */
	semiNaive?: boolean;
}

/**
 * Fixed-Point Operator for Recursive Queries
 *
 * Implements the core fixed-point computation: fix α. F(α)
 * Paper Reference: Proposition 2.9, Definition 2.8 (strictness)
 *
 * @param initialValue - Starting value for the iteration (usually empty or input)
 * @param operation - The recursive operation F where result = F(result)
 * @param options - Configuration options
 * @returns RecursiveResult containing final result and metadata
 */
export function computeFixedPoint(
	initialValue: ZSet,
	operation: (current: ZSet) => ZSet,
	options: RecursiveOptions = {}
): RecursiveResult {
	const maxIters = options.maxIterations ?? 1000;
	const keepTrace = options.keepTrace ?? true;
	const terminationCondition = options.terminationCondition;

	const trace = new Stream();
	let current = initialValue;
	let iteration = 0;

	if (keepTrace) {
		trace.append(current);
	}

	while (iteration < maxIters) {
		const next = operation(current);

		if (keepTrace) {
			trace.append(next);
		}

		// Check for convergence (fixed point reached)
		if (zsetsEqual(current, next)) {
			return {
				result: next,
				iterations: iteration + 1,
				converged: true,
				trace
			};
		}

		// Check custom termination condition
		if (terminationCondition && terminationCondition(iteration, next, current)) {
			return {
				result: next,
				iterations: iteration + 1,
				converged: false, // Terminated early, might not be true fixed point
				trace
			};
		}

		current = next;
		iteration++;
	}

	// Max iterations reached without convergence
	return {
		result: current,
		iterations: iteration,
		converged: false,
		trace
	};
}

/**
 * Streaming Fixed-Point Operator
 *
 * Implements recursive computation on streams using the pattern from Section 5:
 * δ₀ ∘ (fix α. R(I, α)) ∘ ∫
 *
 * This converts a stream of inputs into a stream where each element is the
 * fixed-point computation result for that input.
 *
 * @param inputStream - Stream of inputs to the recursive computation
 * @param recursiveOp - Operation R(input, current) -> next
 * @param options - Configuration options
 * @returns Stream of fixed-point results
 */
export function streamingFixedPoint(
	inputStream: Stream,
	recursiveOp: (input: ZSet, current: ZSet) => ZSet,
	options: RecursiveOptions = {}
): Stream {
	const result = new Stream();

	for (let t = 0; t < inputStream.length; t++) {
		const input = inputStream.get(t);

		// Compute fixed point for this input
		const fixedPointResult = computeFixedPoint(
			input, // Start with the input as initial value
			(current) => recursiveOp(input, current),
			options
		);

		result.append(fixedPointResult.result);
	}

	return result;
}

/**
 * Semi-Naïve Fixed-Point Computation
 *
 * Paper Reference: Section 5.1, semi-naïve evaluation
 * This is more efficient than naïve evaluation as it only processes changes.
 *
 * The key insight: instead of recomputing F(entire_result) at each iteration,
 * we compute F(only_new_facts) and accumulate.
 *
 * @param initialValue - Starting value
 * @param operation - The recursive operation
 * @param options - Configuration options
 * @returns RecursiveResult with optimized computation
 */
export function semiNaiveFixedPoint(
	initialValue: ZSet,
	operation: (current: ZSet, delta: ZSet) => ZSet,
	options: RecursiveOptions = {}
): RecursiveResult {
	const maxIters = options.maxIterations ?? 1000;
	const keepTrace = options.keepTrace ?? true;
	const terminationCondition = options.terminationCondition;

	const trace = new Stream();
	let accumulated = initialValue; // Total accumulated result
	let delta = initialValue; // Changes in this iteration
	let iteration = 0;

	if (keepTrace) {
		trace.append(accumulated);
	}

	while (iteration < maxIters && !delta.isZero()) {
		// Apply operation only to the delta (new facts)
		const newFacts = operation(accumulated, delta);

		// Remove facts we already knew (semi-naïve optimization)
		// actuallyNewFacts = newFacts - accumulated (proper set difference)
		const actuallyNewFacts = new ZSet();
		for (const [key, weight] of newFacts.debug()) {
			if (weight > 0) {
				const existingWeight = accumulated.debug().get(key) || 0;
				if (existingWeight === 0) {
					// This is a completely new fact
					actuallyNewFacts.add(JSON.parse(key), weight);
				} else if (weight > existingWeight) {
					// Increased weight - add the difference
					actuallyNewFacts.add(JSON.parse(key), weight - existingWeight);
				}
			}
		}

		if (actuallyNewFacts.isZero()) {
			// No new facts derived, we've reached fixed point
			return {
				result: accumulated,
				iterations: iteration + 1,
				converged: true,
				trace
			};
		}

		// Accumulate new facts
		accumulated = accumulated.plus(actuallyNewFacts);
		delta = actuallyNewFacts;

		if (keepTrace) {
			trace.append(accumulated);
		}

		// Check custom termination condition
		if (terminationCondition && terminationCondition(iteration, accumulated, delta)) {
			return {
				result: accumulated,
				iterations: iteration + 1,
				converged: false,
				trace
			};
		}

		iteration++;
	}

	return {
		result: accumulated,
		iterations: iteration,
		converged: delta.isZero(), // Converged if no more deltas
		trace
	};
}

/**
 * Transitive Closure Implementation
 *
 * Classic example of recursive query: given edges, compute all reachable pairs.
 * Rule: path(X,Z) :- edge(X,Z) | path(X,Y), edge(Y,Z)
 *
 * This demonstrates the full DBSP recursive pattern.
 *
 * @param edges - Z-set of [source, target] pairs representing edges
 * @param options - Computation options
 * @returns RecursiveResult containing all reachable pairs
 */
export function transitiveClosure(edges: ZSet, options: RecursiveOptions = {}): RecursiveResult {
	const useSemiNaive = options.semiNaive ?? true;

	if (useSemiNaive) {
		return semiNaiveFixedPoint(
			edges, // Start with direct edges
			(accumulated, delta) => {
				// Semi-naïve rule: path(X,Z) :- path(X,Y), edge(Y,Z)
				// Only compute new paths using delta facts

				const newPaths = new ZSet();

				// Rule: Extend delta paths with any existing edge from original edges
				// Only use original edges to avoid double-counting
				for (const [deltaKey, deltaWeight] of delta.debug()) {
					if (deltaWeight <= 0) continue;
					const [deltaStart, deltaEnd] = JSON.parse(deltaKey);

					// Extend delta path with any original edge
					for (const [edgeKey, edgeWeight] of edges.debug()) {
						if (edgeWeight <= 0) continue;
						const [edgeStart, edgeEnd] = JSON.parse(edgeKey);

						if (deltaEnd === edgeStart) {
							// delta path + edge
							newPaths.add([deltaStart, edgeEnd], deltaWeight * edgeWeight);
						}
					}
				}

				return newPaths;
			},
			options
		);
	} else {
		// Naïve evaluation
		return computeFixedPoint(
			edges,
			(current) => {
				// Recompute all paths from scratch each time (less efficient)
				const allPaths = edges.plus(new ZSet()); // Start with edges

				// Add all derived paths
				for (const [path1Key, path1Weight] of current.debug()) {
					if (path1Weight <= 0) continue;
					const [start1, end1] = JSON.parse(path1Key);

					for (const [path2Key, path2Weight] of current.debug()) {
						if (path2Weight <= 0) continue;
						const [start2, end2] = JSON.parse(path2Key);

						if (end1 === start2) {
							allPaths.add([start1, end2], path1Weight * path2Weight);
						}
					}
				}

				return allPaths.distinct(); // Remove duplicates
			},
			options
		);
	}
}

/**
 * Incremental Transitive Closure
 *
 * Implements the full DBSP pattern for incremental recursive queries.
 * When edges are added/removed, efficiently update the transitive closure.
 *
 * Paper Reference: Section 6, incremental recursive programs
 *
 * @param edgeChanges - Stream of edge changes over time
 * @param options - Computation options
 * @returns Stream of transitive closure results
 */
export function incrementalTransitiveClosure(
	edgeChanges: Stream,
	options: RecursiveOptions = {}
): Stream {
	const result = new Stream();
	let currentEdges = new ZSet();

	for (let t = 0; t < edgeChanges.length; t++) {
		// Apply changes to current edge set
		currentEdges = currentEdges.plus(edgeChanges.get(t));

		// Compute transitive closure of updated edges
		const closureResult = transitiveClosure(currentEdges, options);
		result.append(closureResult.result);
	}

	return result;
}

/**
 * Graph Reachability Query
 *
 * Another classic recursive query: given a graph and starting nodes,
 * find all reachable nodes.
 *
 * @param edges - Graph edges as Z-set of [source, target] pairs
 * @param startNodes - Starting nodes as Z-set of node names
 * @param options - Computation options
 * @returns All nodes reachable from start nodes
 */
export function graphReachability(
	edges: ZSet,
	startNodes: ZSet,
	options: RecursiveOptions = {}
): RecursiveResult {
	return semiNaiveFixedPoint(
		startNodes, // Start with the given start nodes
		(accumulated, delta) => {
			const newReachable = new ZSet();

			// For each node we can reach (from delta - new discoveries)
			for (const [nodeKey, nodeWeight] of delta.debug()) {
				if (nodeWeight <= 0) continue;
				const reachableNode = JSON.parse(nodeKey);

				// Find all outgoing edges from this node
				for (const [edgeKey, edgeWeight] of edges.debug()) {
					if (edgeWeight <= 0) continue;
					const [source, target] = JSON.parse(edgeKey);

					if (source === reachableNode) {
						// We can reach the target
						newReachable.add(target, nodeWeight * edgeWeight);
					}
				}
			}

			return newReachable;
		},
		options
	);
}

// Helper function to check Z-set equality
function zsetsEqual(z1: ZSet, z2: ZSet): boolean {
	const debug1 = z1.debug();
	const debug2 = z2.debug();

	if (debug1.size !== debug2.size) return false;

	for (const [key, weight] of debug1) {
		if (debug2.get(key) !== weight) return false;
	}

	return true;
}

/**
 * Utility: Create graph edges from adjacency list
 * Convenience function for testing and examples
 */
export function createGraphFromAdjacencyList(adj: Record<string, string[]>): ZSet {
	const edges = new ZSet();

	for (const [source, targets] of Object.entries(adj)) {
		for (const target of targets) {
			edges.add([source, target], 1);
		}
	}

	return edges;
}

/**
 * Utility: Extract reachable node names from result
 * Helper for working with reachability results
 */
export function extractNodes(reachableZSet: ZSet): Set<string> {
	const nodes = new Set<string>();

	for (const [key, weight] of reachableZSet.debug()) {
		if (weight > 0) {
			const node = JSON.parse(key);
			nodes.add(typeof node === 'string' ? node : node[0]);
		}
	}

	return nodes;
}

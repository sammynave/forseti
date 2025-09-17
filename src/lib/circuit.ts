import { Stream } from './stream.js';
import { delta0, integrate, streamSum } from './stream/utils.js';
import { ZSet } from './z-set.js';
/**
 * Circuit - DBSP Computational Graph Builder
 *
 * The Circuit class is responsible for constructing and executing computational graphs
 * of streaming operations in the DBSP (Database Stream Processing) model. It provides
 * a composable way to build complex data processing pipelines that can operate both
 * in batch and incremental modes.
 *
 * WHY WE NEED IT:
 * - Composability: Build complex queries from simple operators
 * - Incremental Processing: Automatically convert batch queries to incremental ones
 * - Feedback Loops: Support recursive queries and iterative computations
 * - Optimization: Enable query optimization through circuit transformation
 * - Modularity: Encapsulate reusable query patterns
 *
 * WHEN TO USE:
 * - Building complex streaming analytics pipelines
 * - Creating reusable query templates
 * - Implementing recursive algorithms (graph traversal, transitive closure)
 * - Converting batch algorithms to incremental ones
 * - Building real-time dashboards with complex aggregations
 *
 * HOW TO USE:
 * ```typescript
 * const circuit = new Circuit()
 *   .addOperator(stream => stream.liftFilter(user => user.active))
 *   .addOperator(stream => stream.liftProject(user => ({ id: user.id, name: user.name })))
 *   .addOperator(stream => stream.liftDistinct());
 *
 * const result = circuit.execute(userStream);
 *
 * // Or make it incremental for real-time processing
 * const incrementalCircuit = circuit.makeIncremental();
 * const changes = incrementalCircuit.execute(userChangeStream);
 * ```
 *
 * REAL-WORLD EXAMPLE: E-commerce Recommendation Engine
 *
 * Imagine building a real-time product recommendation system that needs to:
 * 1. Track user clicks and purchases
 * 2. Compute user similarity based on behavior
 * 3. Find trending products in user's category preferences
 * 4. Generate personalized recommendations
 *
 * Traditional approach (batch processing):
 * - Run expensive similarity computations every hour
 * - Recommendations become stale between runs
 * - High latency, poor user experience
 * - Doesn't scale with user activity volume
 *
 * With Circuit (incremental processing):
 * ```typescript
 * const recommendationCircuit = new Circuit()
 *   // Filter recent user interactions
 *   .addOperator(stream => stream.liftFilter(event =>
 *     Date.now() - event.timestamp < 3600000)) // Last hour
 *
 *   // Group by user and compute preferences
 *   .addOperator(stream => stream.liftProject(event => ({
 *     userId: event.userId,
 *     category: event.product.category,
 *     weight: event.type === 'purchase' ? 3 : 1
 *   })))
 *
 *   // Add feedback loop for collaborative filtering
 *   .addFeedback(userPrefs =>
 *     userPrefs.liftJoin(userSimilarity,
 *       pref => pref.userId,
 *       sim => sim.userId1))
 *
 *   // Generate top-N recommendations
 *   .addOperator(stream => stream.liftProject(data =>
 *     generateRecommendations(data)));
 *
 * // Convert to incremental for real-time updates
 * const realTimeRecommendations = recommendationCircuit.makeIncremental();
 *
 * // Now every user click/purchase instantly updates recommendations
 * userEventStream.subscribe(event => {
 *   const changeStream = new Stream();
 *   changeStream.append(eventToZSetChange(event));
 *
 *   const updatedRecommendations = realTimeRecommendations.execute(changeStream);
 *   updateUserDashboard(event.userId, updatedRecommendations);
 * });
 * ```
 *
 * Benefits achieved:
 * - Recommendations update in milliseconds, not hours
 * - System scales linearly with user activity
 * - Complex multi-step analytics become composable and testable
 * - Same logic works for both batch initialization and incremental updates
 * - Feedback loops enable sophisticated collaborative filtering
 *
 * The Circuit class transforms what would be a complex, monolithic system into
 * a series of composable, testable, and automatically optimizable components.
 */

export class Circuit {
	private operators: ((input: Stream) => Stream)[] = [];

	static fromQueryOperations(operations: QueryOperation[]): Circuit {
		const circuit = new Circuit();

		for (const op of operations) {
			circuit.addOperationFromQuery(op);
		}

		return circuit;
	}

	private addOperationFromQuery(op: QueryOperation): void {
		// Convert query builder operations to circuit operators
	}

	// Add an operator to the circuit
	addOperator(op: (input: Stream) => Stream): Circuit {
		this.operators.push(op);
		return this; // For chaining
	}

	// Execute the circuit
	execute(input: Stream): Stream {
		return this.operators.reduce((stream, op) => op(stream), input);
	}

	// Make the circuit incremental using Q^Δ = D ∘ Q ∘ I
	makeIncremental(): Circuit {
		const originalExecute = this.execute.bind(this);
		const incrementalCircuit = new Circuit();

		incrementalCircuit.addOperator((deltaStream: Stream) => {
			// Apply Q^Δ = D ∘ Q ∘ I
			const integrated = integrate(deltaStream); // I
			const queryResult = originalExecute(integrated); // Q
			return queryResult.differentiate(); // D
		});

		return incrementalCircuit;
	}
	// Replace the complex version with this simpler, correct one
	addFeedback(feedbackFn: (stream: Stream) => Stream): Circuit {
		const circuit = new Circuit();

		circuit.addOperator((input: Stream) => {
			// Simple fixed-point computation using DBSP semantics
			const result = new Stream();

			// Build result incrementally (causal computation)
			for (let t = 0; t < input.length; t++) {
				if (t === 0) {
					// First element: just the input (no feedback yet)
					result.append(input.get(0));
				} else {
					// Subsequent elements: input + feedback from previous steps
					const partialResult = new Stream();
					for (let i = 0; i < t; i++) {
						partialResult.append(result.get(i));
					}

					const feedback = feedbackFn(partialResult.delay());
					const combined = input.get(t).plus(feedback.length > t ? feedback.get(t) : new ZSet());
					result.append(combined);
				}
			}

			return result;
		});

		return circuit;
	}

	// Helper function to check stream equality (simplified)
	private streamsEqual(s1: Stream, s2: Stream): boolean {
		if (s1.length !== s2.length) return false;
		for (let t = 0; t < s1.length; t++) {
			// Compare materialized forms for simplicity
			const m1 = s1.get(t).materialize;
			const m2 = s2.get(t).materialize;
			if (JSON.stringify(m1) !== JSON.stringify(m2)) return false;
		}
		return true;
	}

	// Support for binary operators like join
	addBinaryOperator(op: (left: Stream, right: Stream) => Stream, rightInput: Stream): Circuit {
		this.addOperator((leftInput: Stream) => op(leftInput, rightInput));
		return this;
	}

	// Support for n-ary operators
	addNaryOperator(op: (...inputs: Stream[]) => Stream, ...otherInputs: Stream[]): Circuit {
		this.addOperator((firstInput: Stream) => op(firstInput, ...otherInputs));
		return this;
	}

	// Support for nested time domains (recursive queries)
	addRecursiveOperator(
		recursiveFn: (input: Stream) => Stream,
		terminationCondition?: (stream: Stream) => boolean
	): Circuit {
		this.addOperator((input: Stream) => {
			// Implement δ₀ ∘ (recursive computation) ∘ ∫ pattern
			const initialStream = delta0(streamSum(input)); // δ₀(∫(input))

			let current = recursiveFn(initialStream);
			let iteration = 0;
			const maxIterations = 100; // Safety limit

			while (iteration < maxIterations) {
				const next = recursiveFn(current);

				if (terminationCondition && terminationCondition(next)) {
					break;
				}

				current = next;
				iteration++;
			}

			return current;
		});

		return this;
	}

	// Debug: show circuit structure
	debug(): string {
		return `Circuit with ${this.operators.length} operators`;
	}

	// Get circuit as a composable function
	asFunction(): (input: Stream) => Stream {
		return (input: Stream) => this.execute(input);
	}

	addDBSPFeedback(
		binaryOp: (input: Stream, feedback: Stream) => Stream,
		feedbackTransform: (stream: Stream) => Stream
	): Circuit {
		const circuit = new Circuit();

		circuit.addOperator((input: Stream) => {
			// Implements: Q(s) = fix α. T(s, F(α)) from Lemma 2.10
			// where T is causal and F is strict

			const combinedOperator = (stream: Stream) => {
				const delayed = stream.delay(); // z⁻¹ (makes it strict)
				const transformed = feedbackTransform(delayed);
				return binaryOp(input, transformed);
			};

			return this.computeFixedPoint(input, combinedOperator);
		});

		return circuit;
	}

	// Add to Circuit class - mathematically rigorous fixed point
	private computeFixedPoint(input: Stream, strictOperator: (stream: Stream) => Stream): Stream {
		// Proposition 2.9: For strict F, equation α = F(α) has unique solution
		const result = new Stream();

		// Compute each time step incrementally using causality
		for (let t = 0; t < input.length; t++) {
			if (t === 0) {
				// F(α)[0] = 0 for strict operators (Definition 2.8)
				result.append(input.get(0));
			} else {
				// F(α)[t] depends only on α[0]...α[t-1] (strictness)
				// Build partial stream up to t-1
				const partialStream = new Stream();
				for (let i = 0; i < t; i++) {
					partialStream.append(result.get(i));
				}

				// Apply operator and get t-th element
				const operatorResult = strictOperator(partialStream);
				if (operatorResult.length > t) {
					result.append(operatorResult.get(t));
				} else {
					result.append(input.get(t)); // Default to zero
				}
			}
		}

		return result;
	}
}

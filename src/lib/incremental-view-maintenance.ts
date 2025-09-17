import { Circuit } from './circuit.js';
import { Stream } from './stream.js';
import { Query, type QueryOperation } from './query-builder.js';

/*
// Example: Real-time user analytics with automatic incrementalization
const userAnalytics = Query
  .from(userChangeStream)                    // Stream of user updates
  .where(user => user.active)                // Only active users
  .select(user => ({                         // Project to needed fields
    id: user.id,
    country: user.country,
    signupDate: user.signupDate
  }))
  .join(                                     // Join with purchase data
    purchaseChangeStream,
    user => user.id,
    purchase => purchase.userId
  )
  .where(([user, purchase]) =>               // Filter recent purchases
    Date.now() - purchase.date < 30 * 24 * 3600 * 1000
  )
  .select(([user, purchase]) => ({           // Final projection
    country: user.country,
    revenue: purchase.amount
  }))
  .autoIncremental();                        // 🎯 Algorithm 4.6 magic!

// The result is an optimized incremental circuit that:
// 1. Only processes changes (not full tables)
// 2. Has optimized distinct operations
// 3. Uses bilinear join optimizations
// 4. Applies chain rule for composition
*/

/**
 * DBSP Algorithm 4.6 - Automatic Incremental View Maintenance
 *
 * This is the core algorithm that transforms any query into its
 * optimal incremental equivalent. Paper Reference: Section 4.3, Algorithm 4.6
 *
 * The 5-step process:
 * 1. Build DBSP circuit from query operations
 * 2. Apply distinct elimination rules (Propositions 4.4, 4.5)
 * 3. Lift circuit to streams (already done in Query Builder)
 * 4. Incrementalize with I and D: Q^Δ = D ∘ Q ∘ I
 * 5. Apply chain rule and optimizations (Proposition 3.2)
 */

export class IncrementalViewMaintenance {
	private currentQueryOperations: QueryOperation[] = [];

	/**
	 * Main API: Generate optimized incremental plan from fluent query
	 *
	 * @param query - Fluent query built with Query Builder
	 * @returns Optimized circuit that processes only changes
	 */
	generateIncrementalPlan<T>(query: Query<T>): Circuit {
		// Step 1: Optimize query operations directly
		const optimizedOperations = this.optimizeQueryOperations(query.getOperations());

		// Store for chain rule optimization
		this.currentQueryOperations = optimizedOperations;

		// Step 2: Apply chain rule optimization (this does the incrementalization)
		const optimizedCircuit = this.applyChainRule(new Circuit());

		return optimizedCircuit;
	}

	/**
	 * Step 2: Optimize query operations directly (before building circuit)
	 */
	private optimizeQueryOperations(operations: readonly QueryOperation[]): QueryOperation[] {
		let optimized = [...operations];

		// Apply distinct elimination rules
		optimized = this.removeRedundantDistinct(optimized);
		optimized = this.pushDistinctThroughLinearOps(optimized);
		optimized = this.consolidateDistinctOps(optimized);

		return optimized;
	}

	/**
	 * Step 5: Apply chain rule and operator-specific optimizations
	 *
	 * Paper Reference: Proposition 3.2 (chain rule properties)
	 * - Linear operators: Q^Δ = Q (Theorem 3.3)
	 * - Bilinear operators: Use Theorem 3.4 formula
	 * - Composition: (Q1 ∘ Q2)^Δ = Q1^Δ ∘ Q2^Δ
	 */
	private applyChainRule(circuit: Circuit): Circuit {
		// Instead of generic incrementalization, we'll build an optimized
		// incremental circuit based on operator types
		const optimizedCircuit = new Circuit();

		// We need the original query operations to determine optimization strategy
		const operations = this.currentQueryOperations; // Store this during optimization

		optimizedCircuit.addOperator((deltaStream: Stream) => {
			return this.executeOptimizedIncremental(deltaStream, operations);
		});

		return optimizedCircuit;
	}

	/**
	 * Execute incremental query with operator-specific optimizations
	 */
	private executeOptimizedIncremental(deltaStream: Stream, operations: QueryOperation[]): Stream {
		let currentStream = deltaStream;

		for (const op of operations) {
			if (op.type === 'source') {
				continue; // Skip source
			}

			// Apply operator-specific incremental version
			currentStream = this.applyIncrementalOperator(currentStream, op);
		}

		return currentStream;
	}

	/**
	 * Apply incremental version of a single operator
	 */
	private applyIncrementalOperator(deltaStream: Stream, operation: QueryOperation): Stream {
		switch (operation.type) {
			case 'filter':
				// Theorem 3.3: Linear operator - Q^Δ = Q
				return deltaStream.liftFilter(operation.predicate!);

			case 'project':
				// Theorem 3.3: Linear operator - Q^Δ = Q
				return deltaStream.liftProject(operation.selector!);

			case 'join':
				// Theorem 3.4: Bilinear operator - use optimized formula
				return this.applyIncrementalJoin(deltaStream, operation);

			case 'distinct':
				// Proposition 4.7: Optimized distinct implementation
				return this.applyIncrementalDistinct(deltaStream);

			case 'union':
				// Linear operator: Q^Δ = Q (just addition)
				return this.applyIncrementalUnion(deltaStream, operation);

			default:
				throw new Error(`Unknown operation type: ${operation.type}`);
		}
	}

	/**
	 * Apply incremental join using Theorem 3.4 optimized formula
	 * (a × b)^Δ = I(a) × b + a × z⁻¹(I(b))
	 */
	private applyIncrementalJoin(deltaStream: Stream, operation: QueryOperation): Stream {
		const otherStream = operation.otherStream!;

		// Use existing optimized join implementation from Stream class
		return deltaStream.liftJoinIncremental(otherStream, operation.thisKey!, operation.otherKey!);
	}

	/**
	 * Apply incremental distinct using Proposition 4.7
	 * This is more efficient than naive D ∘ distinct ∘ I
	 */
	private applyIncrementalDistinct(deltaStream: Stream): Stream {
		// The Stream class already has an optimized distinct implementation
		// For incremental distinct, we need to track the current state

		// For now, use the existing lift distinct - in full implementation,
		// we'd use the optimized incremental distinct from Proposition 4.7
		return deltaStream.liftDistinct();
	}

	/**
	 * Apply incremental union (linear operator)
	 *
	 * TODO: Full union implementation requires:
	 * - Multi-stream input handling
	 * - Stream source resolution
	 * - Time synchronization between different source streams
	 *
	 * For now, throw clear error to indicate this feature is planned but not implemented.
	 */
	private applyIncrementalUnion(deltaStream: Stream, operation: QueryOperation): Stream {
		throw new Error(
			'Union operation not yet implemented in incremental mode. ' +
				'Union requires multi-stream input handling which is planned for a future release. ' +
				'Current supported operations: filter, project, join, distinct.'
		);
	}

	/**
	 * Rule 1: Remove redundant consecutive distinct operations
	 */
	private removeRedundantDistinct(operations: QueryOperation[]): QueryOperation[] {
		const result: QueryOperation[] = [];

		for (let i = 0; i < operations.length; i++) {
			const current = operations[i];
			const next = operations[i + 1];

			// Skip redundant distinct: distinct(distinct(x)) = distinct(x)
			if (current.type === 'distinct' && next?.type === 'distinct') {
				continue; // Skip current, next one will be processed
			}

			result.push(current);
		}

		return result;
	}

	/**
	 * Rule 2: Push distinct through linear operations (Proposition 4.4)
	 */
	private pushDistinctThroughLinearOps(operations: QueryOperation[]): QueryOperation[] {
		const result: QueryOperation[] = [];

		for (let i = 0; i < operations.length; i++) {
			const current = operations[i];
			const next = operations[i + 1];

			// Pattern: filter → distinct becomes distinct → filter
			if (current.type === 'filter' && next?.type === 'distinct') {
				result.push({ ...next }); // distinct first
				result.push({ ...current }); // filter second
				i++; // Skip next
				continue;
			}

			// Similar for project → distinct
			if (current.type === 'project' && next?.type === 'distinct') {
				result.push({ ...next });
				result.push({ ...current });
				i++;
				continue;
			}

			result.push(current);
		}

		return result;
	}

	/**
	 * Rule 3: Consolidate multiple distinct operations (Proposition 4.5)
	 */
	private consolidateDistinctOps(operations: QueryOperation[]): QueryOperation[] {
		// For linear operations between distincts, we can remove the first distinct
		// distinct(Q(distinct(i))) = distinct(Q(i)) where Q is linear

		const result: QueryOperation[] = [];

		for (let i = 0; i < operations.length; i++) {
			const current = operations[i];

			// Look for pattern: distinct → [linear ops] → distinct
			if (current.type === 'distinct') {
				let j = i + 1;
				let allLinear = true;

				// Find next distinct and check if all operations between are linear
				while (j < operations.length && operations[j].type !== 'distinct') {
					if (!this.isLinearOperation(operations[j])) {
						allLinear = false;
						break;
					}
					j++;
				}

				// If we found another distinct and all ops between are linear
				if (j < operations.length && operations[j].type === 'distinct' && allLinear) {
					// Skip this distinct (optimization: remove first distinct)
					continue;
				}
			}

			result.push(current);
		}

		return result;
	}
	/*
	 * Check if operation is linear (can have distinct pushed through it)
	 */
	private isLinearOperation(op: QueryOperation): boolean {
		return ['filter', 'project'].includes(op.type);
	}
}

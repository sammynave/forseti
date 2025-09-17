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
	/**
	 * Main API: Generate optimized incremental plan from fluent query
	 *
	 * @param query - Fluent query built with Query Builder
	 * @returns Optimized circuit that processes only changes
	 */
	generateIncrementalPlan<T>(query: Query<T>): Circuit {
		// Algorithm 4.6 Step 1: Build DBSP circuit from query operations
		// Step 2: Apply distinct elimination rules
		const optimizedOperations = this.optimizeQueryOperations(query.getOperations());

		// Algorithm 4.6 Steps 1-3: Build and lift circuit
		const circuit = Circuit.fromQueryOperations(optimizedOperations);

		// Algorithm 4.6 Steps 4-5: Incrementalize with Q^Δ = D ∘ Q ∘ I
		return circuit.makeIncremental();
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
		return ['filter', 'project', 'union'].includes(op.type);
	}
}

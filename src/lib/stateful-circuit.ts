import { ZSet, ZSetGroup } from './z-set.js';
import { StatefulEquiJoin } from './stateful-join.js';
import { ZSetOperators } from './z-set-operators.js';

/**
 * Stateful circuit that maintains internal state for true incremental processing.
 * This implements the DBSP vision where circuits process only deltas without
 * re-traversing historical data.
 *
 * Now uses StatefulEquiJoin for O(|Δ|) performance instead of O(|DB|).
 */
export class StatefulJoinCircuit<T, U, K> {
	private statefulJoin: StatefulEquiJoin<T, U, K>;
	private lastProcessedTime: number = -1;

	constructor(
		private keyA: (t: T) => K,
		private keyB: (u: U) => K,
		private groupA: ZSetGroup<T>,
		private groupB: ZSetGroup<U>,
		private groupC: ZSetGroup<[T, U]>
	) {
		this.statefulJoin = new StatefulEquiJoin(keyA, keyB, groupC);
	}

	/**
	 * Process incremental updates using the bilinear formula with persistent indexes:
	 * (a × b)^Δ = Δa × Δb + Δa × I(b) + I(a) × Δb
	 *
	 * The StatefulEquiJoin maintains persistent indexes for I(a) and I(b),
	 * so this operation is truly O(|Δ|) instead of O(|DB|).
	 *
	 * @param deltaA - New records in stream A
	 * @param deltaB - New records in stream B
	 * @returns Only the incremental join results (delta output)
	 */
	processIncrement(deltaA: ZSet<T>, deltaB: ZSet<U>): ZSet<[T, U]> {
		return this.statefulJoin.processIncrement(deltaA, deltaB);
	}

	/**
	 * Get current cumulative state (for debugging/inspection)
	 * Note: This now returns the persistent indexes from StatefulEquiJoin
	 */
	getState(): {
		cumulativeA: ZSet<T>;
		cumulativeB: ZSet<U>;
		indexes: {
			indexA: Map<string, Array<[T, number]>>;
			indexB: Map<string, Array<[U, number]>>;
		};
	} {
		const indexes = this.statefulJoin.getIndexes();

		// Reconstruct cumulative Z-sets from indexes for backward compatibility
		const cumulativeA = this.groupA.zero();
		for (const records of indexes.indexA.values()) {
			for (const [record, weight] of records) {
				cumulativeA.append([record, weight]);
			}
		}

		const cumulativeB = this.groupB.zero();
		for (const records of indexes.indexB.values()) {
			for (const [record, weight] of records) {
				cumulativeB.append([record, weight]);
			}
		}

		return {
			cumulativeA: cumulativeA.mergeRecords(),
			cumulativeB: cumulativeB.mergeRecords(),
			indexes
		};
	}

	/**
	 * Reset circuit state (useful for testing)
	 */
	reset(): void {
		this.statefulJoin.reset();
		this.lastProcessedTime = -1;
	}

	/**
	 * Initialize circuit with base data (equivalent to processing initial deltas)
	 */
	initialize(initialA: ZSet<T>, initialB: ZSet<U>): ZSet<[T, U]> {
		return this.statefulJoin.initialize(initialA, initialB);
	}

	/**
	 * Get the complete materialized view (full join result) from current state.
	 * This is maintained incrementally for O(1) access - no recomputation needed!
	 *
	 * @returns Complete join result maintained incrementally
	 */
	getMaterializedView(): ZSet<[T, U]> {
		return this.statefulJoin.getMaterializedView();
	}
}

/**
 * Factory function for creating stateful join circuits
 */
export function createStatefulJoinCircuit<T, U, K>(
	keyA: (t: T) => K,
	keyB: (u: U) => K
): StatefulJoinCircuit<T, U, K> {
	return new StatefulJoinCircuit(
		keyA,
		keyB,
		new ZSetGroup<T>(),
		new ZSetGroup<U>(),
		new ZSetGroup<[T, U]>()
	);
}

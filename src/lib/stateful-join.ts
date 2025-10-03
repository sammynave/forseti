import { ZSet, ZSetGroup } from './z-set.js';

/**
 * Stateful equi-join operator that maintains persistent indexes for true O(|Δ|) incremental performance.
 *
 * This implements the DBSP bilinear join formula:
 * (a × b)^Δ = Δa × Δb + Δa × I(b) + I(a) × Δb
 *
 * Where:
 * - Δa, Δb are the input deltas
 * - I(a), I(b) are the integrated (cumulative) relations maintained as persistent indexes
 */
export class StatefulEquiJoin<T, U, K> {
	// Persistent indexes - these are the I(a) and I(b) from the DBSP formula
	private indexA: Map<string, Array<[T, number]>> = new Map();
	private indexB: Map<string, Array<[U, number]>> = new Map();

	// Materialized view maintained as a Map for O(1) updates
	private materializedViewMap: Map<string, [[T, U], number]> = new Map();

	constructor(
		private keyExtractorA: (record: T) => K,
		private keyExtractorB: (record: U) => K,
		private groupC: ZSetGroup<[T, U]>
	) {
		// No longer need to initialize materializedView ZSet
	}

	/**
	 * Process incremental join using persistent indexes.
	 * Only processes the delta records, not the entire dataset.
	 *
	 * @param deltaA - New/changed records in relation A
	 * @param deltaB - New/changed records in relation B
	 * @returns Only the incremental join results (delta output)
	 */
	processIncrement(deltaA: ZSet<T>, deltaB: ZSet<U>): ZSet<[T, U]> {
		let deltaResult = this.groupC.zero();

		// Term 1: Δa × Δb (new A × new B)
		if (!deltaA.isEmpty() && !deltaB.isEmpty()) {
			const term1 = this.joinZSets(deltaA, deltaB);
			deltaResult = this.groupC.add(deltaResult, term1);
		}

		// Term 2: Δa × I(b) (new A × existing B)
		// Join deltaA against persistent indexB
		if (!deltaA.isEmpty()) {
			const term2 = this.joinDeltaAgainstIndex(deltaA, this.indexB, this.keyExtractorA);
			deltaResult = this.groupC.add(deltaResult, term2);
		}

		// Term 3: I(a) × Δb (existing A × new B)
		// Join persistent indexA against deltaB
		if (!deltaB.isEmpty()) {
			const term3 = this.joinIndexAgainstDelta(this.indexA, deltaB, this.keyExtractorB);
			deltaResult = this.groupC.add(deltaResult, term3);
		}

		// Update persistent indexes with deltas (this is the integration step)
		this.updateIndex(this.indexA, deltaA, this.keyExtractorA);
		this.updateIndex(this.indexB, deltaB, this.keyExtractorB);

		// Update materialized view incrementally using efficient map-based approach
		this.updateMaterializedView(deltaResult);

		return deltaResult;
	}

	/**
	 * Standard join between two Z-sets (used for Δa × Δb term)
	 */
	private joinZSets(a: ZSet<T>, b: ZSet<U>): ZSet<[T, U]> {
		const result = this.groupC.zero();

		// Build temporary index for b
		const tempIndexB = new Map<string, Array<[U, number]>>();
		for (const [recordB, weightB] of b.data) {
			const keyB = this.keyExtractorB(recordB);
			const keyStr = JSON.stringify(keyB);
			if (!tempIndexB.has(keyStr)) {
				tempIndexB.set(keyStr, []);
			}
			tempIndexB.get(keyStr)!.push([recordB, weightB]);
		}

		// Join a against temporary index
		for (const [recordA, weightA] of a.data) {
			const keyA = this.keyExtractorA(recordA);
			const keyStr = JSON.stringify(keyA);
			const matchingBRecords = tempIndexB.get(keyStr) || [];

			for (const [recordB, weightB] of matchingBRecords) {
				const combinedWeight = weightA * weightB;
				if (combinedWeight !== 0) {
					result.append([[recordA, recordB], combinedWeight]);
				}
			}
		}

		return result.mergeRecords();
	}

	/**
	 * Join delta A against persistent index B (for Δa × I(b) term)
	 */
	private joinDeltaAgainstIndex(
		deltaA: ZSet<T>,
		indexB: Map<string, Array<[U, number]>>,
		keyExtractor: (record: T) => K
	): ZSet<[T, U]> {
		const result = this.groupC.zero();

		for (const [recordA, weightA] of deltaA.data) {
			const keyA = keyExtractor(recordA);
			const keyStr = JSON.stringify(keyA);
			const matchingBRecords = indexB.get(keyStr) || [];

			for (const [recordB, weightB] of matchingBRecords) {
				const combinedWeight = weightA * weightB;
				if (combinedWeight !== 0) {
					result.append([[recordA, recordB], combinedWeight]);
				}
			}
		}

		return result.mergeRecords();
	}

	/**
	 * Join persistent index A against delta B (for I(a) × Δb term)
	 */
	private joinIndexAgainstDelta(
		indexA: Map<string, Array<[T, number]>>,
		deltaB: ZSet<U>,
		keyExtractor: (record: U) => K
	): ZSet<[T, U]> {
		const result = this.groupC.zero();

		for (const [recordB, weightB] of deltaB.data) {
			const keyB = keyExtractor(recordB);
			const keyStr = JSON.stringify(keyB);
			const matchingARecords = indexA.get(keyStr) || [];

			for (const [recordA, weightA] of matchingARecords) {
				const combinedWeight = weightA * weightB;
				if (combinedWeight !== 0) {
					result.append([[recordA, recordB], combinedWeight]);
				}
			}
		}

		return result.mergeRecords();
	}

	/**
	 * Update persistent index with delta records (incremental integration)
	 * This is much more efficient than rebuilding the entire index
	 */
	private updateIndex<V>(
		index: Map<string, Array<[V, number]>>,
		delta: ZSet<V>,
		keyExtractor: (record: V) => K
	): void {
		for (const [record, weight] of delta.data) {
			const key = keyExtractor(record);
			const keyStr = JSON.stringify(key);

			if (!index.has(keyStr)) {
				index.set(keyStr, []);
			}

			// Add the delta record to the index
			// Note: In a full implementation, we'd need to handle weight consolidation
			// For now, we append and rely on mergeRecords() in the join operations
			index.get(keyStr)!.push([record, weight]);
		}
	}

	/**
	 * Update materialized view incrementally using efficient map-based approach
	 * This avoids the expensive ZSet.add() operations on large datasets
	 */
	private updateMaterializedView(deltaResult: ZSet<[T, U]>): void {
		for (const [record, weight] of deltaResult.data) {
			const key = JSON.stringify(record);

			if (this.materializedViewMap.has(key)) {
				const [existingRecord, existingWeight] = this.materializedViewMap.get(key)!;
				const newWeight = existingWeight + weight;

				if (newWeight === 0) {
					this.materializedViewMap.delete(key);
				} else {
					this.materializedViewMap.set(key, [existingRecord, newWeight]);
				}
			} else if (weight !== 0) {
				this.materializedViewMap.set(key, [record, weight]);
			}
		}
	}

	/**
	 * Get current state of persistent indexes (for debugging/inspection)
	 */
	getIndexes(): {
		indexA: Map<string, Array<[T, number]>>;
		indexB: Map<string, Array<[U, number]>>;
	} {
		return {
			indexA: this.indexA,
			indexB: this.indexB
		};
	}

	/**
	 * Get the current materialized view (complete join result).
	 * This is maintained incrementally for O(1) access.
	 */
	getMaterializedView(): ZSet<[T, U]> {
		// Convert map back to ZSet only when requested
		const data = Array.from(this.materializedViewMap.values());
		return new ZSet(data);
	}

	/**
	 * Reset all persistent state
	 */
	reset(): void {
		this.indexA.clear();
		this.indexB.clear();
		this.materializedViewMap.clear();
	}

	/**
	 * Initialize with base data (equivalent to processing initial deltas)
	 */
	initialize(initialA: ZSet<T>, initialB: ZSet<U>): ZSet<[T, U]> {
		return this.processIncrement(initialA, initialB);
	}
}

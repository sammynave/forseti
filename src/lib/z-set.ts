// Z-sets generalize database tables: think of a Z-set as a table where

import type { AbelianGroup } from './operators/integrate.js';

// NOTE: these are instance methods. we will need some state when we get
// to non-linear aggregation, like `MIN`
/*
Section 7.2
"One way to implement in DBSP the lifted incremental version of such aggregate
functions is by 'brute force', using the formula (↑a_MIN)^Δ = D ∘ ↑a_MIN ∘ I.
Such an implementations performs work O(|DB|) at each invocation.
However, schemes such as Reactive Aggregator can be implemented as custom DBSP
operators to bring the amortized cost per update to O(log |DB|)."
*/
export class ZSetGroup<T> implements AbelianGroup<ZSet<T>> {
	// 0 = {}(empty);
	zero(): ZSet<T> {
		return new ZSet([]);
	}

	// A = {(x, 3), (y, -1)}
	// B = {(x, 2), (z, 4)}
	// A + B = {(x, 5), (y, -1), (z, 4)}
	// (a + b)[x] = a[x] + b[x]
	add(a: ZSet<T>, b: ZSet<T>): ZSet<T> {
		// Early exit optimizations
		if (a.isEmpty()) return b;
		if (b.isEmpty()) return a;

		// If both are already merged, we can do a more efficient merge
		if (a.isMerged() && b.isMerged()) {
			return this.fastMerge(a, b);
		}

		return a.concat(b).mergeRecords();
	}

	// Efficient merge for already-merged ZSets
	private fastMerge(a: ZSet<T>, b: ZSet<T>): ZSet<T> {
		// For now, fall back to the standard merge approach
		// TODO: Implement proper fast merge with public key access methods
		return a.concat(b).mergeRecords();
	}

	// (a - b)[x] = a[x] - b[x]
	subtract(a: ZSet<T>, b: ZSet<T>): ZSet<T> {
		return this.add(a, this.negate(b));
	}

	// ALGEBRA
	// A = {(x, 3), (y, -1)}
	// -A = {(x, -3), (y, 1)}
	// (-a)[x] = -a[x]
	negate(a: ZSet<T>): ZSet<T> {
		return new ZSet(a.data.map(([r, w]) => [r, -w]));
	}
}

// each row has an associated weight, possibly negative.
export class ZSet<T> {
	#data: Array<[T, number]> = [];
	#keyCache: Map<T, string> = new Map();
	#isMerged: boolean = false;

	constructor(data: Array<[T, number]>) {
		this.#data = data;
	}

	get data() {
		return this.#data;
	}

	append(d: [T, number]) {
		this.#data.push(d);
		this.#isMerged = false; // Mark as unmerged when adding data
	}

	isMerged(): boolean {
		return this.#isMerged;
	}

	private getOrComputeKey(record: T): string {
		if (this.#keyCache.has(record)) {
			return this.#keyCache.get(record)!;
		}
		const key = JSON.stringify(record);
		this.#keyCache.set(record, key);
		return key;
	}

	// type predicates
	isSet() {
		return this.#data.every(([_, w]) => w === 1);
	}
	isPositive() {
		return this.#data.every(([_, w]) => w >= 0);
	}
	isEmpty() {
		return this.#data.length === 0;
	}

	// Merges the same records and adds multiplicities
	mergeRecords(): ZSet<T> {
		if (this.#isMerged) {
			return this; // Already merged, return self
		}

		const mergedRecords = new Map<string, [T, number]>(); // key -> [record, totalWeight]

		for (const [record, weight] of this.#data) {
			// Use cached key computation to avoid repeated JSON.stringify
			const key = this.getOrComputeKey(record);

			if (mergedRecords.has(key)) {
				const [existingRecord, existingWeight] = mergedRecords.get(key)!;
				mergedRecords.set(key, [existingRecord, existingWeight + weight]);
			} else {
				mergedRecords.set(key, [record, weight]);
			}
		}

		// Build result ZSet with non-zero weights
		const result = new ZSet<T>([]);
		for (const [record, weight] of mergedRecords.values()) {
			if (weight !== 0) {
				result.append([record, weight]);
				// Transfer cached key to result
				const key = this.getOrComputeKey(record);
				result.#keyCache.set(record, key);
			}
		}

		result.#isMerged = true;
		return result;
	}

	// ZSet<1> * 2 => ZSet<2>
	// (k * a)[x] = k * a[x]
	multiply(scalar: number) {
		return this.#data.reduce((acc, [r, w]) => {
			const newW = w * scalar;
			if (newW === 0) return acc;

			acc.append([r, newW]);
			return acc;
		}, new ZSet([]));
	}

	concat(other: ZSet<T>): ZSet<T> {
		const unioned = new ZSet<T>([]);
		for (const d of this.#data) {
			unioned.append(d);
		}
		for (const d of other.data) {
			unioned.append(d);
		}

		return unioned;
	}
}
// Group for tuples (needed for bilinear operators)
export class TupleGroup<A, B> implements AbelianGroup<[A, B]> {
	constructor(
		private groupA: AbelianGroup<A>,
		private groupB: AbelianGroup<B>
	) {}

	zero(): [A, B] {
		return [this.groupA.zero(), this.groupB.zero()];
	}

	add(a: [A, B], b: [A, B]): [A, B] {
		return [this.groupA.add(a[0], b[0]), this.groupB.add(a[1], b[1])];
	}

	subtract(a: [A, B], b: [A, B]): [A, B] {
		return [this.groupA.subtract(a[0], b[0]), this.groupB.subtract(a[1], b[1])];
	}

	negate(a: [A, B]): [A, B] {
		return [this.groupA.negate(a[0]), this.groupB.negate(a[1])];
	}
}

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

	constructor(data: Array<[T, number]>) {
		this.#data = data;
	}

	get data() {
		return this.#data;
	}
	append(d: [T, number]) {
		this.#data.push(d);
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

	// Aggregates
	count() {}
	sum() {}
	min() {}
	max() {}

	// Merges the same records and adds multiplicities
	mergeRecords(): ZSet<T> {
		const mergedRecords = new Map<string, number>();
		const jsonToValue = new Map<string, T>();

		for (const [r, w] of this.#data) {
			// @PERF - is it okay to stringify everything? is there a faster way?
			// is there a case for WeakMap here? if primitive, compare directly,
			// if complex, murmur hash or something?
			const key = JSON.stringify(r);
			jsonToValue.set(key, r);
			mergedRecords.set(key, (mergedRecords.get(key) ?? 0) + w);
		}

		const reducer = (acc: ZSet<T>, [r, w]: [string, number]) => {
			if (w === 0) return acc;
			const key = jsonToValue.get(r);
			if (!key) return acc;

			acc.append([key, w]);
			return acc;
		};

		return Array.from(mergedRecords.entries()).reduce(reducer, new ZSet([]));
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

	// Linear operation: Preserves weights
	filter(pred: (i: T) => boolean): ZSet<T> {
		return new ZSet(this.#data.filter(([r]) => pred(r)));
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

	// RELATIONAL
	// distinct(m)[x] = 1 if m[x] > 0, else 0
	distinct() {
		const map = this.mergeRecords().data.reduce((acc, [r, w]) => {
			if (w < 1) return acc;
			if (acc.has(r)) return acc;

			acc.set(r, 1);
			return acc;
		}, new Map());
		return new ZSet(Array.from(map.entries()));
	}

	//  Bilinear operation for combining Z-sets
	// `(a ⊲⊳ b)[(x,y)] = a[x] × b[y]` if join condition met
	join(other: ZSet<T>): ZSet<T> {}
}

// Z-sets generalize database tables: think of a Z-set as a table where
// each row has an associated weight, possibly negative.
export class ZSet<T> {
	#data: Array<[T, number]> = [];

	// 0 = {}(empty);
	static zero() {
		return new ZSet([]);
	}

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

	// ALGEBRA
	// A = {(x, 3), (y, -1)}
	// -A = {(x, -3), (y, 1)}
	// (-a)[x] = -a[x]
	negate() {
		return new ZSet(this.#data.map(([r, w]) => [r, -w]));
	}

	// Merges the same records and adds multiplicities
	mergeRecords(): ZSet<T> {
		const mergedRecords = new Map();
		const jsonToValue = new Map();

		for (const [r, w] of this.#data) {
			// @PERF - is it okay to stringify everything? is there a faster way?
			const key = JSON.stringify(r);
			jsonToValue.set(key, r);
			mergedRecords.set(key, mergedRecords.has(key) ? mergedRecords.get(key) + w : w);
		}

		const reducer = (acc, [r, w]) => {
			if (w === 0) return acc;
			acc.append([jsonToValue.get(r), w]);
			return acc;
		};

		return Array.from(mergedRecords.entries()).reduce(reducer, new ZSet([]));
	}

	// A = {(x, 3), (y, -1)}
	// B = {(x, 2), (z, 4)}
	// A + B = {(x, 5), (y, -1), (z, 4)}
	// (a + b)[x] = a[x] + b[x]
	add(other: ZSet<T>): ZSet<T | this> {
		return this.concat(other).mergeRecords();
	}

	// (a - b)[x] = a[x] - b[x]
	subtract(other: ZSet<T>): ZSet<T | this> {
		return this.add(other.negate());
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

	concat(other: ZSet<T>): ZSet<T | this> {
		const unioned = new ZSet<T | this>([]);
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

export class ZSet {
	private data = new Map<string, number>();

	// DBSP-style: add element with specified weight (default +1 for insertion)
	add(item: unknown, weight: number = 1) {
		const key = JSON.stringify(item);
		const currentWeight = this.data.get(key) || 0;
		const newWeight = currentWeight + weight;

		// DBSP semantics: keep all weights, including 0
		this.data.set(key, newWeight);
	}

	plus(other: ZSet): ZSet {
		const result = new ZSet();

		// Get all unique keys from both Z-sets
		const allKeys = new Set([...this.data.keys(), ...other.data.keys()]);

		// For each key, add weights from both Z-sets
		for (const key of allKeys) {
			const thisWeight = this.data.get(key) ?? 0;
			const otherWeight = other.data.get(key) ?? 0;
			const combinedWeight = thisWeight + otherWeight;

			result.data.set(key, combinedWeight);
		}

		return result;
	}

	zero(): ZSet {
		return new ZSet(); // Empty Z-set is the identity
	}

	negate(): ZSet {
		const result = new ZSet();
		for (const [key, weight] of this.data) {
			result.data.set(key, -weight);
		}
		return result;
	}

	// Convert to regular Todo array for display (only positive weights)
	get materialize() {
		return Array.from(this.data.entries())
			.filter(([_, weight]) => weight > 0)
			.map(([todoJson, _]) => JSON.parse(todoJson));
	}
	/* END Maybe these should move */

	// Debug: show the raw Z-set data
	debug(): Map<string, number> {
		return new Map(this.data);
	}
}

// Stream<A> represents a function ℕ → A (natural numbers to values of type A)
// s[t] gives the value at time t
// Following DBSP Definition 2.1
export class Stream<A> {
	private values: A[] = [];

	// Get value at time t: s[t]
	get(t: number): A {
		if (t < 0 || !Number.isInteger(t)) {
			throw new Error(`Invalid time ${t}: must be non-negative integer`);
		}
		if (t >= this.values.length) {
			throw new Error(`Stream not defined at time ${t}`);
		}
		return this.values[t];
	}

	// Set value at time t (for constructing streams)
	set(t: number, value: A): void {
		if (t < 0 || !Number.isInteger(t)) {
			throw new Error(`Invalid time ${t}: must be non-negative integer`);
		}

		// Extend array if needed
		while (this.values.length <= t) {
			this.values.push(undefined as any);
		}
		this.values[t] = value;
	}

	// Append value at next time step (convenience method)
	append(value: A): number {
		const t = this.values.length;
		this.values.push(value);
		return t;
	}

	// Get current length (highest defined time + 1)
	get length(): number {
		return this.values.length;
	}

	// Check if time t is defined
	isDefined(t: number): boolean {
		return t >= 0 && Number.isInteger(t) && t < this.values.length;
	}
}

export function integrate(changes: Stream<ZSet>): Stream<ZSet> {
	const result = new Stream<ZSet>();
	let accumulator = new ZSet(); // Start with empty Z-set

	for (let t = 0; t < changes.length; t++) {
		accumulator = accumulator.plus(changes.get(t));
		result.set(t, accumulator);
	}

	return result;
}

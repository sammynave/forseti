import { Stream } from './stream.js';
import { ZSet } from './z-set.js';

/**
 * NestedStream (SS_A) - Streams of Streams for Recursive Queries
 *
 * DBSP Definition: SS_A = N → (N → A) (nested streams)
 * Paper Reference: Section 6, Page 17
 *
 * A nested stream can be thought of as a matrix with infinite rows and columns,
 * where each cell contains a value of type A. The first dimension represents
 * "outer time" and the second dimension represents "inner time".
 *
 * This is essential for recursive queries where we have:
 * - Outer time: iterations of incremental updates to input relations
 * - Inner time: iterations of fixed-point computation within each update
 *
 * WHY WE NEED IT:
 * - Recursive queries (transitive closure, graph reachability)
 * - Stratified Datalog implementation
 * - Incremental maintenance of recursive views
 * - Semi-naïve evaluation with proper incremental semantics
 *
 * REAL-WORLD EXAMPLE: Social Network Friend Recommendations
 *
 * Consider maintaining "friends of friends" recommendations incrementally:
 * - Outer time: user adds/removes friends (input changes)
 * - Inner time: computing transitive closure of friendship graph
 *
 * When Alice adds Bob as friend:
 * - Outer time t=5: new friendship edge added
 * - Inner time within t=5:
 *   - τ=0: process direct friendship Alice-Bob
 *   - τ=1: discover Alice-Charlie via Alice-Bob-Charlie
 *   - τ=2: discover Alice-Dave via Alice-Bob-Charlie-Dave
 *   - τ=3: fixed point reached (no new connections)
 *
 * NestedStream[5][2] would contain the friendship discoveries at outer time 5,
 * inner iteration 2 of the fixed-point computation.
 */
export class NestedStream {
	private data: Stream[] = [];

	/**
	 * Get the stream at outer time t0
	 * @param t0 - Outer time index
	 * @returns Stream at that outer time
	 */
	getStream(t0: number): Stream {
		if (t0 < 0 || !Number.isInteger(t0)) {
			throw new Error(`Invalid outer time ${t0}: must be non-negative integer`);
		}
		if (t0 >= this.data.length) {
			return new Stream(); // Return empty stream for undefined times
		}
		return this.data[t0];
	}

	/**
	 * Set the stream at outer time t0
	 * @param t0 - Outer time index
	 * @param stream - Stream to set
	 */
	setStream(t0: number, stream: Stream): void {
		if (t0 < 0 || !Number.isInteger(t0)) {
			throw new Error(`Invalid outer time ${t0}: must be non-negative integer`);
		}

		// Fill gaps with empty streams
		while (this.data.length <= t0) {
			this.data.push(new Stream());
		}
		this.data[t0] = stream;
	}

	/**
	 * Get value at nested coordinates (t0, t1)
	 * @param t0 - Outer time index
	 * @param t1 - Inner time index
	 * @returns Value at nested time coordinates
	 */
	get(t0: number, t1: number): ZSet {
		const stream = this.getStream(t0);
		return stream.get(t1);
	}

	/**
	 * Set value at nested coordinates (t0, t1)
	 * @param t0 - Outer time index
	 * @param t1 - Inner time index
	 * @param value - Value to set
	 */
	set(t0: number, t1: number, value: ZSet): void {
		if (t0 >= this.data.length) {
			// Ensure we have a stream at t0
			while (this.data.length <= t0) {
				this.data.push(new Stream());
			}
		}
		this.data[t0].set(t1, value);
	}

	/**
	 * Add a stream as the next outer time step
	 * @param stream - Stream to append
	 * @returns The outer time index where it was added
	 */
	appendStream(stream: Stream): number {
		const t0 = this.data.length;
		this.data.push(stream);
		return t0;
	}

	/**
	 * Get the number of outer time steps
	 */
	get outerLength(): number {
		return this.data.length;
	}

	/**
	 * Get the maximum inner length across all outer times
	 */
	get maxInnerLength(): number {
		let max = 0;
		for (const stream of this.data) {
			max = Math.max(max, stream.length);
		}
		return max;
	}

	/**
	 * Check if the nested stream is zero (all elements are zero Z-sets)
	 */
	isZero(): boolean {
		for (const stream of this.data) {
			if (!stream.isZero()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Create a zero nested stream
	 */
	static zero(): NestedStream {
		return new NestedStream();
	}

	/**
	 * Nested stream addition (componentwise)
	 * Paper Reference: Section 6 - nested streams form abelian groups
	 */
	plus(other: NestedStream): NestedStream {
		const result = new NestedStream();
		const maxOuterLength = Math.max(this.outerLength, other.outerLength);

		for (let t0 = 0; t0 < maxOuterLength; t0++) {
			const thisStream = t0 < this.outerLength ? this.getStream(t0) : new Stream();
			const otherStream = t0 < other.outerLength ? other.getStream(t0) : new Stream();
			result.setStream(t0, thisStream.plus(otherStream));
		}

		return result;
	}

	/**
	 * Nested stream negation
	 */
	negate(): NestedStream {
		const result = new NestedStream();
		for (let t0 = 0; t0 < this.outerLength; t0++) {
			result.setStream(t0, this.getStream(t0).negate());
		}
		return result;
	}

	/**
	 * Nested lifting: applies stream operator to each row (outer time)
	 *
	 * Paper Reference: Section 6 - ↑S : SS_A → SS_B
	 * This is lifting a stream operator S to work on nested streams
	 *
	 * @param streamOp - Stream operator to lift
	 * @returns Nested stream with operator applied to each row
	 */
	liftStreamOperator(streamOp: (stream: Stream) => Stream): NestedStream {
		const result = new NestedStream();
		for (let t0 = 0; t0 < this.outerLength; t0++) {
			result.setStream(t0, streamOp(this.getStream(t0)));
		}
		return result;
	}

	/**
	 * Nested delay (↑z^(-1)): delays columns, not rows
	 *
	 * Paper Reference: Section 6 - nested delay operates on inner time dimension
	 * Unlike regular delay which delays the stream itself, nested delay delays
	 * the contents of each stream (the columns of the matrix)
	 *
	 * @returns Nested stream with each inner stream delayed
	 */
	nestedDelay(): NestedStream {
		const result = new NestedStream();
		for (let t0 = 0; t0 < this.outerLength; t0++) {
			result.setStream(t0, this.getStream(t0).delay());
		}
		return result;
	}

	/**
	 * Nested integration (↑I): integrates each column independently
	 *
	 * Paper Reference: Section 6 - operates on columns of the matrix
	 * This integrates along the inner time dimension for each outer time
	 */
	nestedIntegrate(): NestedStream {
		const result = new NestedStream();
		for (let t0 = 0; t0 < this.outerLength; t0++) {
			// Import integrate function
			// result.setStream(t0, integrate(this.getStream(t0)));
			// For now, implement inline to avoid circular imports
			const changes = this.getStream(t0);
			const integrated = new Stream();
			let accumulator = new ZSet();

			for (let t = 0; t < changes.length; t++) {
				accumulator = accumulator.plus(changes.get(t));
				integrated.append(accumulator);
			}
			result.setStream(t0, integrated);
		}
		return result;
	}

	/**
	 * Nested differentiation (↑D): differentiates each column independently
	 */
	nestedDifferentiate(): NestedStream {
		const result = new NestedStream();
		for (let t0 = 0; t0 < this.outerLength; t0++) {
			result.setStream(t0, this.getStream(t0).differentiate());
		}
		return result;
	}

	/**
	 * Extract a specific column (inner time dimension)
	 * Useful for debugging and analysis
	 *
	 * @param t1 - Inner time to extract
	 * @returns Stream containing values from all outer times at inner time t1
	 */
	extractColumn(t1: number): Stream {
		const result = new Stream();
		for (let t0 = 0; t0 < this.outerLength; t0++) {
			result.append(this.get(t0, t1));
		}
		return result;
	}

	/**
	 * Debug representation showing the matrix structure
	 */
	debug(): string {
		const lines = [`NestedStream (${this.outerLength} × ${this.maxInnerLength}):`];

		for (let t0 = 0; t0 < Math.min(this.outerLength, 5); t0++) {
			// Show first 5 rows
			const stream = this.getStream(t0);
			const rowData = [];
			for (let t1 = 0; t1 < Math.min(stream.length, 5); t1++) {
				// Show first 5 columns
				const zset = stream.get(t1);
				if (zset.isZero()) {
					rowData.push('∅');
				} else {
					rowData.push(`{${zset.materialize.length}}`);
				}
			}
			if (stream.length > 5) rowData.push('...');
			lines.push(`  t0=${t0}: [${rowData.join(', ')}]`);
		}

		if (this.outerLength > 5) {
			lines.push('  ...');
		}

		return lines.join('\n');
	}

	/**
	 * Check if nested stream dimensions match another nested stream
	 * Useful for operations requiring compatible dimensions
	 */
	dimensionsMatch(other: NestedStream): boolean {
		if (this.outerLength !== other.outerLength) return false;

		for (let t0 = 0; t0 < this.outerLength; t0++) {
			if (this.getStream(t0).length !== other.getStream(t0).length) {
				return false;
			}
		}

		return true;
	}

	/**
	 * Convert to regular stream by flattening (for debugging/testing)
	 * This loses the nested structure but can be useful for simple cases
	 */
	flatten(): Stream {
		const result = new Stream();

		for (let t0 = 0; t0 < this.outerLength; t0++) {
			const stream = this.getStream(t0);
			for (let t1 = 0; t1 < stream.length; t1++) {
				result.append(stream.get(t1));
			}
		}

		return result;
	}
}

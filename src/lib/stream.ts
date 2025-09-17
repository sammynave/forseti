// Stream<A> represents a function ℕ → A (natural numbers to values of type A)
// s[t] gives the value at time t

import { integrate } from './stream/utils.js';
import { ZSet } from './z-set.js';

// Following DBSP Definition 2.1
export class Stream {
	private values: ZSet[] = [];

	get(t: number): ZSet {
		if (t < 0 || !Number.isInteger(t)) {
			throw new Error(`Invalid time ${t}: must be non-negative integer`);
		}
		if (t >= this.values.length) {
			return new ZSet();
		}
		return this.values[t];
	}

	set(t: number, value: ZSet): void {
		if (t < 0 || !Number.isInteger(t)) {
			throw new Error(`Invalid time ${t}: must be non-negative integer`);
		}

		// Fill gaps with zero (empty ZSet)
		while (this.values.length <= t) {
			this.values.push(new ZSet()); // Zero element
		}
		this.values[t] = value;
	}

	isZero(): boolean {
		// Zero stream: all elements are zero Z-sets
		for (let t = 0; t < this.length; t++) {
			if (!this.get(t).isZero()) {
				// Assuming ZSet has isZero() method
				return false;
			}
		}
		return true;
	}

	static zero(): Stream {
		const result = new Stream();
		// Zero stream has no elements (all values are 0_A)
		return result;
	}

	append(value: ZSet): number {
		const t = this.values.length;
		this.values.push(value);
		return t;
	}

	// DBSP operators
	lift(f: (zset: ZSet) => ZSet): Stream {
		const result = new Stream();
		for (let t = 0; t < this.length; t++) {
			result.append(f(this.get(t)));
		}
		return result;
	}

	plus(other: Stream): Stream {
		const result = new Stream();
		const maxLength = Math.max(this.length, other.length);

		for (let t = 0; t < maxLength; t++) {
			const thisVal = t < this.length ? this.get(t) : new ZSet();
			const otherVal = t < other.length ? other.get(t) : new ZSet();
			result.append(thisVal.plus(otherVal));
		}
		return result;
	}

	negate(): Stream {
		return this.lift((zset) => zset.negate());
	}

	get length(): number {
		return this.values.length;
	}

	// Check if time t is defined
	isDefined(t: number): boolean {
		return t >= 0 && Number.isInteger(t) && t < this.values.length;
	}

	/**
	 * Stream Differentiation Operator (D)
	 *
	 * DBSP Definition 2.15: D(s)[t] = s[t] - s[t-1] for t > 0, D(s)[0] = s[0]
	 * Paper Reference: Section 2.3, Definition 2.15
	 *
	 * This is the PRECISE implementation from the paper. Note that D(s)[0] = s[0]
	 * directly, NOT s[0] - 0. This is critical for Theorem 2.20 (inversion property)
	 * and all incremental computation correctness.
	 *
	 * Properties:
	 * - Causal: output at time t depends only on inputs up to time t
	 * - Linear: D(s + t) = D(s) + D(t)
	 * - Time-Invariant: D(z⁻¹(s)) = z⁻¹(D(s))
	 * - Inverse of Integration: I(D(s)) = D(I(s)) = s
	 *
	 * @returns Stream of differences representing rate of change
	 */
	differentiate(): Stream {
		const result = new Stream();

		for (let t = 0; t < this.length; t++) {
			if (t === 0) {
				// DBSP Definition 2.15: D(s)[0] = s[0] (NOT s[0] - 0)
				result.append(this.get(0));
			} else {
				// D(s)[t] = s[t] - s[t-1] for t > 0
				const current = this.get(t);
				const previous = this.get(t - 1);
				result.append(current.plus(previous.negate()));
			}
		}

		return result;
	}

	delay(): Stream {
		const result = new Stream();

		// First element is always zero
		result.append(new ZSet()); // 0_A

		// Subsequent elements are shifted by one
		for (let t = 0; t < this.length; t++) {
			result.append(this.get(t));
		}

		return result;
	}

	/**
	 * Applies incremental view maintenance to a query over a stream of changes.
	 *
	 * This is the core of DBSP's incremental computation model. Instead of recomputing
	 * the entire query result from scratch each time data changes, this method:
	 * 1. Integrates the change stream into snapshots (I)
	 * 2. Applies the query to those snapshots (Q)
	 * 3. Differentiates the result back to changes (D)
	 *
	 * The formula Q^Δ = D ∘ Q ∘ I transforms any "batch" query Q into its incremental
	 * equivalent Q^Δ that processes only changes, not full datasets.
	 *
	 * WHY IT'S NEEDED:
	 * - Enables real-time analytics on streaming data
	 * - Avoids expensive full recomputation on every update
	 * - Maintains correctness while dramatically improving performance
	 * - Allows complex queries (joins, aggregations) to run incrementally
	 *
	 * REAL-WORLD EXAMPLE:
	 * Consider a live dashboard showing "top 10 products by sales in the last hour".
	 *
	 * WITHOUT incremental processing:
	 * - Every new sale → recompute entire query over all recent sales
	 * - 1000 sales/minute = 1000 full table scans + sorts per minute
	 * - Performance degrades as data grows
	 *
	 * WITH applyIncremental:
	 * - New sale comes in as a change (+1 to product X)
	 * - Only update the running top-10 list incrementally
	 * - Constant time per update regardless of total data size
	 * - Dashboard stays responsive even with millions of historical sales
	 *
	 * The method automatically handles the complex logic of maintaining consistency
	 * between the change stream and the materialized view.
	 */
	applyIncremental(queryFn: (stream: Stream) => Stream): Stream {
		// Q^Δ = D ∘ Q ∘ I
		const integrated = integrate(this); // I
		const queryResult = queryFn(integrated); // Q
		return queryResult.differentiate(); // D
	}
	// Lifted relational operators for streams
	liftFilter(predicate: (item: any) => boolean): Stream {
		return this.lift((zset) => zset.filter(predicate));
	}

	liftProject<T>(selector: (item: any) => T): Stream {
		return this.lift((zset) => zset.project(selector));
	}

	liftJoin<K>(other: Stream, thisKey: (item: any) => K, otherKey: (item: any) => K): Stream {
		const result = new Stream();
		const maxLength = Math.max(this.length, other.length);

		for (let t = 0; t < maxLength; t++) {
			const thisZSet = t < this.length ? this.get(t) : new ZSet();
			const otherZSet = t < other.length ? other.get(t) : new ZSet();
			result.append(thisZSet.join(otherZSet, thisKey, otherKey));
		}

		return result;
	}

	/**
	 * Incremental bilinear join operator implementing Theorem 3.4
	 *
	 * For bilinear operators like join: (a × b)^Δ = I(a) × b + a × z^(-1)(I(b))
	 * This is much more efficient than the naive D ∘ (↑×) ∘ I approach
	 *
	 * @param other - The other stream to join with
	 * @param thisKey - Key extraction function for this stream
	 * @param otherKey - Key extraction function for other stream
	 * @returns Incremental join result stream
	 */
	liftJoinIncremental<K>(
		other: Stream,
		thisKey: (item: any) => K,
		otherKey: (item: any) => K
	): Stream {
		const result = new Stream();

		// We need integrated versions of both streams
		const integratedThis = integrate(this); // I(a)
		const integratedOther = integrate(other); // I(b)
		const delayedIntegratedOther = integratedOther.delay(); // z^(-1)(I(b))

		// The result length should match the maximum of all intermediate streams
		const maxLength = Math.max(
			this.length,
			other.length,
			integratedThis.length,
			integratedOther.length,
			delayedIntegratedOther.length
		);

		for (let t = 0; t < maxLength; t++) {
			// Get current values
			const thisZSet = t < this.length ? this.get(t) : new ZSet();
			const otherZSet = t < other.length ? other.get(t) : new ZSet();

			// Get integrated values
			const integratedThisZSet = t < integratedThis.length ? integratedThis.get(t) : new ZSet();
			const delayedIntegratedOtherZSet =
				t < delayedIntegratedOther.length ? delayedIntegratedOther.get(t) : new ZSet();

			// Apply Theorem 3.4: (a × b)^Δ = I(a) × b + a × z^(-1)(I(b))
			const term1 = integratedThisZSet.join(otherZSet, thisKey, otherKey); // I(a) × b
			const term2 = thisZSet.join(delayedIntegratedOtherZSet, thisKey, otherKey); // a × z^(-1)(I(b))

			result.append(term1.plus(term2));
		}

		return result;
	}

	/**
	 * Incremental bilinear cartesian product implementing Theorem 3.4
	 *
	 * @param other - The other stream for cartesian product
	 * @returns Incremental cartesian product result stream
	 */
	liftCartesianProductIncremental(other: Stream): Stream {
		const result = new Stream();

		const integratedThis = integrate(this);
		const integratedOther = integrate(other);
		const delayedIntegratedOther = integratedOther.delay();

		// The result length should match the maximum of all intermediate streams
		const maxLength = Math.max(
			this.length,
			other.length,
			integratedThis.length,
			integratedOther.length,
			delayedIntegratedOther.length
		);

		for (let t = 0; t < maxLength; t++) {
			const thisZSet = t < this.length ? this.get(t) : new ZSet();
			const otherZSet = t < other.length ? other.get(t) : new ZSet();

			const integratedThisZSet = t < integratedThis.length ? integratedThis.get(t) : new ZSet();
			const delayedIntegratedOtherZSet =
				t < delayedIntegratedOther.length ? delayedIntegratedOther.get(t) : new ZSet();

			// Apply Theorem 3.4 for cartesian product
			const term1 = integratedThisZSet.cartesianProduct(otherZSet); // I(a) × b
			const term2 = thisZSet.cartesianProduct(delayedIntegratedOtherZSet); // a × z^(-1)(I(b))

			result.append(term1.plus(term2));
		}

		return result;
	}

	liftDistinct(): Stream {
		return this.lift((zset) => zset.distinct());
	}
}

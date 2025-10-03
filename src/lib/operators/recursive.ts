import type { StreamOperator } from '$lib/stream.js';
import { Stream } from '$lib/stream.js';
import type { AbelianGroup } from './integrate.js';

/**
 * Delta function: δ₀(v)[t] = { v if t = 0, 0_A otherwise }
 *
 * Paper Citation: Section 5 - "Stream introduction"
 * Used for recursive queries to inject scalar values into streams
 */
export function delta0<A>(group: AbelianGroup<A>) {
	return (value: A): Stream<A> => {
		const output = new Stream<A>(group.zero());
		output.set(0, value);
		return output;
	};
}

/**
 * Definite integral: ∫(s) = Σ(t≥0) s[t] (for streams zero almost-everywhere)
 *
 * Paper Citation: Section 5 - "Stream elimination"
 * Used for recursive queries to extract final results from streams
 * Note: This assumes the stream becomes zero after some finite time
 */
export function definiteIntegral<A>(group: AbelianGroup<A>) {
	return (input: Stream<A>): A => {
		let sum = group.zero();
		for (const [_, value] of input.entries()) {
			sum = group.add(sum, value);
		}
		return sum;
	};
}

/**
 * Nested streams support (for recursive queries)
 *
 * Paper Citation: Section 6 - "Incremental Recursive Programs"
 * Nested streams SS_A = N → (N → A) are used for recursive computations
 */
export type NestedStream<A> = Stream<Stream<A>>;

/**
 * Lift a stream operator to work on nested streams
 *
 * Paper Citation: Section 6 - "Lifting cycles"
 * (↑S)(s) = S ∘ s, or pointwise: (↑S(s))[t₀][t₁] = S(s[t₀])[t₁]
 */
export function liftToNested<A, B>(
	op: StreamOperator<A, B>,
	groupB: AbelianGroup<B>
): StreamOperator<Stream<A>, Stream<B>> {
	return (nestedInput: Stream<Stream<A>>) => {
		const output = new Stream<Stream<B>>(new Stream<B>(groupB.zero()));

		for (const [time, innerStream] of nestedInput.entries()) {
			output.set(time, op(innerStream));
		}

		return output;
	};
}

/**
 * Fixed-point operator for recursive computations
 *
 * Paper Citation: Proposition 2.9 - "For a strict F : S_A → S_A the equation α = F(α) has a unique solution"
 * This is essential for implementing recursive queries like transitive closure
 */
export function fixedPoint<A>(group: AbelianGroup<A>, maxIterations: number = 1000) {
	return (f: StreamOperator<A, A>) =>
		(input: Stream<A>): Stream<A> => {
			let current = new Stream<A>(group.zero());
			let iteration = 0;

			// Iterate until fixed point or max iterations
			while (iteration < maxIterations) {
				const next = f(current);

				// Check if we've reached a fixed point (simplified check)
				let hasChanged = false;
				const currentEntries = Array.from(current.entries());
				const nextEntries = Array.from(next.entries());

				if (currentEntries.length !== nextEntries.length) {
					hasChanged = true;
				} else {
					for (let i = 0; i < currentEntries.length; i++) {
						const [t1, v1] = currentEntries[i];
						const [t2, v2] = nextEntries[i];
						if (t1 !== t2 || !isEqual(v1, v2)) {
							hasChanged = true;
							break;
						}
					}
				}

				if (!hasChanged) {
					break; // Fixed point reached
				}

				current = next;
				iteration++;
			}

			return current;
		};
}

// Helper function for equality checking (simplified)
function isEqual<T>(a: T, b: T): boolean {
	return JSON.stringify(a) === JSON.stringify(b);
}

/**
 * Stream that is zero almost everywhere check
 *
 * Paper Citation: Definition 5.1 - "We say that a stream s ∈ S_A is zero almost-everywhere
 * if it has a finite number of non-zero values"
 */
export function isZeroAlmostEverywhere<A>(
	stream: Stream<A>,
	group: AbelianGroup<A>,
	maxCheck: number = 1000
): boolean {
	let zeroCount = 0;
	let totalChecked = 0;

	// Check if stream becomes zero after some point
	for (const [time, value] of stream.entries()) {
		if (totalChecked >= maxCheck) break;

		if (isEqual(value, group.zero())) {
			zeroCount++;
		}
		totalChecked++;
	}

	// If we've seen mostly zeros in recent entries, assume it's zero almost-everywhere
	return totalChecked === 0 || zeroCount / totalChecked > 0.9;
}

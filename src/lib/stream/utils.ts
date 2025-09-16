import { Stream } from '$lib/stream.js';
import { ZSet } from '$lib/z-set.js';

/**
 * Stream Elimination Operator ∫ (definite integral)
 *
 * DBSP Definition: ∫(s) = Σ_{t≥0} s[t]
 * Paper Reference: Section 5, Definition after 5.1
 *
 * Converts a stream that is "zero almost-everywhere" to a scalar value.
 * A stream is zero almost-everywhere if it has a finite number of non-zero values.
 *
 * @param stream - Input stream (should be zero almost-everywhere)
 * @param terminationCondition - Optional early termination condition for optimization
 * @returns Accumulated sum of all stream elements
 */
export function integral(stream: Stream, terminationCondition?: (zset: ZSet) => boolean): ZSet {
	let accumulator = new ZSet();
	let consecutiveZeros = 0;
	const maxConsecutiveZeros = 10; // Practical limit for "almost everywhere" detection

	for (let t = 0; t < stream.length; t++) {
		const current = stream.get(t);

		// Add current element to accumulator
		accumulator = accumulator.plus(current);

		// Track consecutive zeros for early termination
		if (current.isZero()) {
			consecutiveZeros++;
		} else {
			consecutiveZeros = 0;
		}

		// Early termination conditions
		if (terminationCondition && terminationCondition(current)) {
			break;
		}

		// If we see many consecutive zeros, assume we've reached the "almost everywhere" part
		if (consecutiveZeros >= maxConsecutiveZeros) {
			break;
		}
	}

	return accumulator;
}

/**
 * Validates if a stream is "zero almost-everywhere"
 * Definition 5.1: A stream that has a finite number of non-zero values
 *
 * @param stream - Stream to validate
 * @param lookAheadLimit - How many elements to check (practical bound)
 * @returns true if stream appears to be zero almost-everywhere
 */
export function isZeroAlmostEverywhere(stream: Stream, lookAheadLimit: number = 100): boolean {
	let nonZeroCount = 0;
	let consecutiveZeros = 0;
	const maxConsecutiveZeros = 20;

	const checkLimit = Math.min(stream.length, lookAheadLimit);

	for (let t = 0; t < checkLimit; t++) {
		const current = stream.get(t);

		if (!current.isZero()) {
			nonZeroCount++;
			consecutiveZeros = 0;
		} else {
			consecutiveZeros++;
		}

		// If we see many consecutive zeros after some non-zeros, likely "almost everywhere"
		if (nonZeroCount > 0 && consecutiveZeros >= maxConsecutiveZeros) {
			return true;
		}
	}

	// If we've checked the entire (finite) stream, it's definitely "almost everywhere"
	return checkLimit === stream.length;
}

// Alias for backward compatibility - using existing streamSum name
export function streamSum(stream: Stream): ZSet {
	return integral(stream);
}
export function delta0(value: ZSet): Stream {
	const result = new Stream();
	result.append(value); // t=0 gets the value
	// All other times implicitly get zero (empty ZSet)
	return result;
}

export function integrate(changes: Stream): Stream {
	const result = new Stream();
	let accumulator = new ZSet(); // Start with zero

	for (let t = 0; t < changes.length; t++) {
		accumulator = accumulator.plus(changes.get(t));
		result.append(accumulator);
	}

	return result;
}

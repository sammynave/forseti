import type { StreamOperator } from '$lib/stream.js';
import { Stream } from '$lib/stream.js';

export interface AbelianGroup<T> {
	zero(): T;
	add(a: T, b: T): T;
	subtract(a: T, b: T): T;
	negate(a: T): T;
}

// Integration: I(s)[t] = Σ(i≤t) s[i]
export function OLDintegrate<A>(group: AbelianGroup<A>): StreamOperator<A, A> {
	return (input: Stream<A>) => {
		const output = new Stream<A>(group.zero());
		let accumulator = group.zero();

		// Compute cumulative sum
		for (const [t, v] of input.entries()) {
			accumulator = group.add(accumulator, v);
			output.set(t, accumulator);
		}

		return output;
	};
}
export function integrate<A>(group: AbelianGroup<A>): StreamOperator<A, A> {
	return (input: Stream<A>) => {
		const output = new Stream<A>(group.zero());
		let accumulator = group.zero();

		// Find the maximum time in input to know how far to integrate
		const maxTime = Math.max(...Array.from(input.entries()).map(([t]) => t), -1);

		// Integrate over ALL time steps from 0 to maxTime
		for (let t = 0; t <= maxTime; t++) {
			const inputValue = input.at(t); // This should return group.zero() for unset times
			accumulator = group.add(accumulator, inputValue);
			output.set(t, accumulator);
		}

		return output;
	};
}

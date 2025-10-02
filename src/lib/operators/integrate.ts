import type { StreamOperator } from '$lib/stream.js';
import { Stream } from '$lib/stream.js';

export interface AbelianGroup<T> {
	zero(): T;
	add(a: T, b: T): T;
	subtract(a: T, b: T): T;
	negate(a: T): T;
}

// Integration: I(s)[t] = Σ(i≤t) s[i]
export function integrate<A>(group: AbelianGroup<A>): StreamOperator<A, A> {
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

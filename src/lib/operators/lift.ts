import type { StreamOperator } from '$lib/stream.js';
import { Stream } from '$lib/stream.js';

// Lifting: ↑f - apply function f to each stream value
export function lift<A, B>(f: (a: A) => B): StreamOperator<A, B> {
	return (input: Stream<A>) => {
		const output = new Stream<B>(f(input.at(0))); // Default from f(default)

		// Apply f to each time point
		for (const [time, value] of input.entries()) {
			output.set(time, f(value));
		}

		return output;
	};
}

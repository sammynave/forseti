import type { StreamOperator } from '$lib/stream.js';
import { Stream } from '$lib/stream.js';

// Delay: z^(-1) - delay stream by one time step
export function delay<A>(defaultValue: A): StreamOperator<A, A> {
	return (input: Stream<A>) => {
		const output = new Stream<A>(defaultValue);

		// s[t-1] with s[-1] = defaultValue
		output.set(0, defaultValue);
		for (const [time, value] of input.entries()) {
			output.set(time + 1, value);
		}

		return output;
	};
}

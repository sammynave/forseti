import type { StreamOperator } from '$lib/stream.js';
import { Stream } from '$lib/stream.js';
import type { AbelianGroup } from './integrate.js';

// Delay: z^(-1) - delay stream by one time step
// DBSP Spec: z^(-1)(s)[t] = { 0_A if t = 0, s[t-1] if t ≥ 1 }
export function delay<A>(group: AbelianGroup<A>): StreamOperator<A, A> {
	return (input: Stream<A>) => {
		const output = new Stream<A>(group.zero());

		// z^(-1)(s)[0] = 0_A (group zero)
		output.set(0, group.zero());

		// z^(-1)(s)[t] = s[t-1] for t ≥ 1
		for (const [time, value] of input.entries()) {
			output.set(time + 1, value);
		}

		return output;
	};
}

import { Stream } from '$lib/stream.js';
import type { StreamOperator } from '$lib/stream.js';
import type { AbelianGroup } from './integrate.js';

// Differentiation: D(s)[t] = s[t] - s[t-1]
export function differentiate<A>(group: AbelianGroup<A>): StreamOperator<A, A> {
	return (input: Stream<A>) => {
		const output = new Stream<A>(group.zero());

		for (const [t, v] of input.entries()) {
			const previous = t > 0 ? input.at(t - 1) : group.zero();
			output.set(t, group.subtract(v, previous));
		}

		return output;
	};
}

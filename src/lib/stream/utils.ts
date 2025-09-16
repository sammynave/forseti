import { Stream } from '$lib/stream.js';
import { ZSet } from '$lib/z-set.js';

// Static function - reduces stream to scalar
export function streamSum(stream: Stream): ZSet {
	let accumulator = new ZSet();

	for (let t = 0; t < stream.length; t++) {
		accumulator = accumulator.plus(stream.get(t));
	}

	return accumulator;
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

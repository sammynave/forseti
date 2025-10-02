import { differentiate } from './operators/differentiate.js';
import { integrate, type AbelianGroup } from './operators/integrate.js';

/**
 * Stream operators: S_A → S_B
 * Pure functions that transform one stream into another
 */
export type StreamOperator<A, B> = (input: Stream<A>) => Stream<B>;

export class Stream<Z> {
	private values = new Map<number, Z>();
	private currentTime = 0;
	private groupZero: Z;

	constructor(groupZero: Z) {
		this.groupZero = groupZero;
	}

	at(time: number): Z {
		return this.values.get(time) ?? this.groupZero; // Use group zero, not arbitrary default
	}

	// Set value at specific time (for construction)
	set(time: number, value: Z): void {
		this.values.set(time, value);
		this.currentTime = Math.max(this.currentTime, time + 1);
	}

	// Get current time (highest set time + 1)
	getCurrentTime(): number {
		return this.currentTime;
	}

	// Iterate over all set values
	*entries(): IterableIterator<[number, Z]> {
		for (const [time, value] of this.values.entries()) {
			yield [time, value];
		}
	}
}

export function incrementalize<A, B>(
	query: StreamOperator<A, B>,
	groupA: AbelianGroup<A>,
	groupB: AbelianGroup<B>
): StreamOperator<A, B> {
	return (deltaStream: Stream<A>) => {
		// Q^Δ = D ∘ Q ∘ I
		const integrated = integrate<A>(groupA)(deltaStream);
		const queried = query(integrated);
		const differentiated = differentiate(groupB)(queried);
		return differentiated;
	};
}

// Helper for creating tuple streams
export function createTupleStream<A, B>(
	streamA: Stream<A>,
	streamB: Stream<B>,
	defaultA: A,
	defaultB: B
): Stream<[A, B]> {
	const result = new Stream<[A, B]>([defaultA, defaultB]);

	const allTimes = new Set<number>();
	for (const [time] of streamA.entries()) allTimes.add(time);
	for (const [time] of streamB.entries()) allTimes.add(time);

	for (const time of allTimes) {
		result.set(time, [streamA.at(time), streamB.at(time)]);
	}

	return result;
}

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
	private defaultValue: Z;

	constructor(defaultValue: Z) {
		this.defaultValue = defaultValue;
	}

	at(time: number): Z {
		return this.values.get(time) ?? this.defaultValue;
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

import { ZSet, ZSetGroup } from './z-set.js';

export class StatefulTopK<T> {
	private sortedElements: Array<[T, number]> = [];
	private elementIndex = new Map<string, number>();
	private lastTopK: ZSet<T>; // Track previous state for delta computation

	constructor(
		private comparator: (a: T, b: T) => number,
		private limit: number = Infinity,
		private offset: number = 0,
		private groupT: ZSetGroup<T>
	) {
		this.lastTopK = this.groupT.zero();
	}

	processIncrement(delta: ZSet<T>): ZSet<T> {
		let resultDelta = this.groupT.zero();

		for (const [record, weight] of delta.data) {
			const key = JSON.stringify(record);
			const currentIndex = this.elementIndex.get(key);

			if (weight > 0) {
				// Add/update element
				this.insertElement(record, weight, currentIndex);
			} else {
				// Remove/decrease element
				this.removeElement(record, -weight, currentIndex);
			}
		}

		// Generate output delta from current top-k
		const newTopK = this.getCurrentTopK();
		const deltaOutput = this.computeOutputDelta(newTopK);

		this.lastTopK = newTopK;
		return deltaOutput;
	}

	private insertElement(record: T, weight: number, currentIndex?: number) {
		const key = JSON.stringify(record);

		if (currentIndex !== undefined) {
			// Update existing
			this.sortedElements[currentIndex][1] += weight;
			if (this.sortedElements[currentIndex][1] <= 0) {
				this.sortedElements.splice(currentIndex, 1);
				this.elementIndex.delete(key);
			}
			this.reSort();
		} else {
			// Insert new
			this.sortedElements.push([record, weight]);
			this.sortedElements.sort(([a], [b]) => this.comparator(a, b));
			this.updateIndexMap();
		}
	}

	private updateIndexMap(): void {
		this.elementIndex.clear();
		this.sortedElements.forEach(([record], index) => {
			const key = JSON.stringify(record);
			this.elementIndex.set(key, index);
		});
	}

	private reSort(): void {
		this.sortedElements.sort(([a], [b]) => this.comparator(a, b));
		this.updateIndexMap();
	}

	private removeElement(record: T, weight: number, currentIndex?: number): void {
		if (currentIndex === undefined) return; // Element not found

		const key = JSON.stringify(record);
		this.sortedElements[currentIndex][1] -= weight;

		if (this.sortedElements[currentIndex][1] <= 0) {
			this.sortedElements.splice(currentIndex, 1);
			this.elementIndex.delete(key);
			this.updateIndexMap(); // Reindex after removal
		}
	}

	private getCurrentTopK(): ZSet<T> {
		const end = Math.min(this.offset + this.limit, this.sortedElements.length);
		const topKData: Array<[T, number]> = this.sortedElements
			.slice(this.offset, end)
			.filter(([_, weight]) => weight > 0)
			.map(([record, weight]) => [record, Math.min(weight, 1)]); // Convert to set semantics

		return new ZSet(topKData);
	}

	private computeOutputDelta(newTopK: ZSet<T>): ZSet<T> {
		// Compute difference: what was added/removed from top-k
		const groupT = this.groupT;

		// Elements in new but not in old = additions (positive weight)
		// Elements in old but not in new = removals (negative weight)
		const additions = groupT.subtract(newTopK, this.lastTopK);
		return additions;
	}

	// Public method for getting current state (useful for testing)
	getCurrentState(): {
		topK: ZSet<T>;
		allElements: Array<[T, number]>;
		size: number;
	} {
		return {
			topK: this.getCurrentTopK(),
			allElements: [...this.sortedElements],
			size: this.sortedElements.length
		};
	}

	reset(): void {
		this.sortedElements = [];
		this.elementIndex.clear();
		this.lastTopK = this.groupT.zero();
	}
}

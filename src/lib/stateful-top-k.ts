import { ZSet, ZSetGroup } from './z-set.js';

export class StatefulTopK<T> {
	private sortedElements: Array<[T, number]> = [];
	private elementIndex = new Map<string, number>();
	private lastTopK: ZSet<T>; // Track previous state for delta computation

	// NEW: Caching infrastructure
	private objectKeyCache = new WeakMap<object, string>(); // For object identity
	private stringKeyCache = new Map<string, string>(); // For content-based keys
	private keyFunction?: (record: T) => string; // Optional custom key function

	constructor(
		private comparator: (a: T, b: T) => number,
		private limit: number = Infinity,
		private offset: number = 0,
		private groupT: ZSetGroup<T>,
		keyFunction?: (record: T) => string // NEW: Optional key function
	) {
		this.lastTopK = this.groupT.zero();
		this.keyFunction = keyFunction;
	}

	// NEW: Optimized key generation with hybrid caching
	private getKey(record: T): string {
		// Strategy 1: Use custom key function if provided (fastest for structured data)
		if (this.keyFunction) {
			return this.keyFunction(record);
		}

		// Strategy 2: Use WeakMap for object identity (fastest for repeated objects)
		if (typeof record === 'object' && record !== null) {
			let cached = this.objectKeyCache.get(record);
			if (cached !== undefined) {
				return cached;
			}
		}

		// Strategy 3: Fallback to JSON.stringify with string cache
		const jsonKey = JSON.stringify(record);
		let cached = this.stringKeyCache.get(jsonKey);
		if (cached !== undefined) {
			return cached;
		}

		// Store in appropriate cache for future use
		if (typeof record === 'object' && record !== null) {
			this.objectKeyCache.set(record, jsonKey);
		}
		this.stringKeyCache.set(jsonKey, jsonKey);

		return jsonKey;
	}

	/**
	 * Optimized bulk loading for initial large datasets
	 * Avoids O(n²) performance of individual insertElement calls
	 */
	processInitial(delta: ZSet<T>): ZSet<T> {
		// Fast path: if empty, we can optimize heavily
		if (this.sortedElements.length === 0) {
			return this.bulkInitialize(delta);
		}

		// Fallback to incremental for non-empty state
		return this.processIncrement(delta);
	}

	private bulkInitialize(delta: ZSet<T>): ZSet<T> {
		// 1. Collect all unique records with aggregated weights
		const recordMap = new Map<string, [T, number]>();

		for (const [record, weight] of delta.data) {
			const key = this.getKey(record); // Use optimized key function
			const existing = recordMap.get(key);

			if (existing) {
				existing[1] += weight; // Aggregate weights
			} else {
				recordMap.set(key, [record, weight]);
			}
		}

		// 2. Filter out zero/negative weights and convert to array
		const validRecords = Array.from(recordMap.values()).filter(([_, weight]) => weight > 0);

		// 3. Sort ONCE (instead of N times)
		validRecords.sort(([a], [b]) => this.comparator(a, b));

		// 4. Build sorted array and index map in one pass
		this.sortedElements = validRecords;
		this.elementIndex.clear();

		validRecords.forEach(([record], index) => {
			const key = this.getKey(record);
			this.elementIndex.set(key, index);
		});

		// 5. Generate output delta
		const newTopK = this.getCurrentTopK();
		const deltaOutput = this.computeOutputDelta(newTopK);

		this.lastTopK = newTopK;
		return deltaOutput;
	}

	processIncrement(delta: ZSet<T>): ZSet<T> {
		let resultDelta = this.groupT.zero();

		for (const [record, weight] of delta.data) {
			const key = this.getKey(record); // CHANGED: Use cached key
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
		const key = this.getKey(record); // CHANGED: Use cached key

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
			const key = this.getKey(record); // CHANGED: Use cached key
			this.elementIndex.set(key, index);
		});
	}

	private reSort(): void {
		this.sortedElements.sort(([a], [b]) => this.comparator(a, b));
		this.updateIndexMap();
	}

	private removeElement(record: T, weight: number, currentIndex?: number): void {
		if (currentIndex === undefined) return; // Element not found

		const key = this.getKey(record); // CHANGED: Use cached key
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
		// NEW: Clear caches
		this.objectKeyCache = new WeakMap();
		this.stringKeyCache.clear();
	}

	// NEW: Public method to get cache statistics (for debugging/monitoring)
	getCacheStats(): {
		stringCacheSize: number;
		objectCacheUsage: string; // WeakMap doesn't expose size
	} {
		return {
			stringCacheSize: this.stringKeyCache.size,
			objectCacheUsage: 'WeakMap size not accessible'
		};
	}
}

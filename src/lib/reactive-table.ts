import { ZSet, ZSetGroup } from './z-set.js';

/**
 * ReactiveTable provides a familiar, high-level interface over ZSets.
 * Users can perform CRUD operations without knowing about ZSets, weights, or deltas.
 */
export class ReactiveTable<T extends Record<string, any>> {
	private data: Map<any, T> = new Map(); // id -> record
	private subscribers = new Set<(delta: ZSet<T>) => void>();
	private group = new ZSetGroup<T>();

	constructor(
		initialData: T[],
		private idField: keyof T
	) {
		// Initialize internal data map
		for (const item of initialData) {
			this.data.set(item[this.idField], item);
		}
	}

	/**
	 * Subscribe to changes. Callback receives ZSet deltas for incremental processing.
	 * Returns unsubscribe function.
	 */
	subscribe(callback: (delta: ZSet<T>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	/**
	 * Get current snapshot of all data as array
	 */
	toArray(): T[] {
		return Array.from(this.data.values());
	}

	/**
	 * Get current data as ZSet (for internal library use)
	 */
	toZSet(): ZSet<T> {
		return new ZSet(this.toArray().map((item) => [item, 1]));
	}

	/**
	 * Add a new record. Automatically notifies subscribers with delta.
	 */
	add(item: T): void {
		const id = item[this.idField];

		if (this.data.has(id)) {
			throw new Error(`Record with id ${id} already exists. Use update() instead.`);
		}

		this.data.set(id, item);

		// Create delta and notify subscribers
		const delta = new ZSet([[item, 1]]);
		this.notifySubscribers(delta);
	}

	/**
	 * Update an existing record. Automatically handles the old/new record delta.
	 */
	update(id: any, changes: Partial<T>): void {
		const oldItem = this.data.get(id);
		if (!oldItem) {
			throw new Error(`Record with id ${id} not found. Use add() instead.`);
		}

		const newItem = { ...oldItem, ...changes } as T;
		this.data.set(id, newItem);

		// Create delta: remove old, add new
		const delta = new ZSet([
			[oldItem, -1], // Remove old
			[newItem, 1] // Add new
		]);
		this.notifySubscribers(delta);
	}

	/**
	 * Remove a record by ID. Automatically notifies subscribers with delta.
	 */
	remove(id: any): boolean {
		const item = this.data.get(id);
		if (!item) {
			return false; // Item not found
		}

		this.data.delete(id);

		// Create delta and notify subscribers
		const delta = new ZSet([[item, -1]]);
		this.notifySubscribers(delta);

		return true;
	}

	/**
	 * Get a record by ID
	 */
	get(id: any): T | undefined {
		return this.data.get(id);
	}

	/**
	 * Check if a record exists
	 */
	has(id: any): boolean {
		return this.data.has(id);
	}

	/**
	 * Get count of records
	 */
	get size(): number {
		return this.data.size;
	}

	private notifySubscribers(delta: ZSet<T>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

/**
 * Factory function to create a reactive table
 */
export function createReactiveTable<T extends Record<string, any>>(
	initialData: T[],
	idField: keyof T
): ReactiveTable<T> {
	return new ReactiveTable(initialData, idField);
}

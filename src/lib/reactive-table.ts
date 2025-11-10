import { ZSet, ZSetGroup } from './z-set.js';

/**
 * Operation types for batch processing
 */
export type BatchOperation<T> =
	| { type: 'upsert'; item: T }
	| { type: 'remove'; id: any }
	| { type: 'delta'; delta: ZSet<T> };

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

	// ===============================
	// IDEMPOTENT OPERATIONS
	// For optimistic updates + SQLite persistence
	// ===============================

	/**
	 * Upsert (insert or update) a record. Idempotent operation that never throws.
	 *
	 * Perfect for optimistic updates with eventual SQLite persistence:
	 * 1. Call upsert() immediately for optimistic UI update
	 * 2. Send same operation to SQLite worker
	 * 3. Worker confirms with same upsert() call - no conflicts
	 *
	 * @param item The record to insert or update
	 *
	 * @example
	 * // Optimistic update pattern:
	 * const user = { id: 1, name: 'Alice', email: 'alice@example.com' };
	 *
	 * // 1. Immediate optimistic update
	 * userTable.upsert(user);
	 *
	 * // 2. Send to worker (your code)
	 * worker.postMessage({ type: 'upsert', data: user });
	 *
	 * // 3. Worker confirms (your code handles this)
	 * worker.onmessage = (e) => {
	 *   if (e.data.type === 'upsert_confirmed') {
	 *     userTable.upsert(e.data.item); // Idempotent - no error
	 *   }
	 * };
	 */
	upsert(item: T): void {
		const id = item[this.idField];
		const oldItem = this.data.get(id);

		// Always update the data
		this.data.set(id, item);

		let delta: ZSet<T>;
		if (oldItem) {
			// Update case: remove old, add new
			delta = new ZSet([
				[oldItem, -1], // Remove old
				[item, 1] // Add new
			]);
		} else {
			// Insert case: just add new
			delta = new ZSet([[item, 1]]);
		}

		this.notifySubscribers(delta);
	}

	/**
	 * Safely remove a record by ID. Idempotent operation that never throws.
	 *
	 * Safe to call multiple times - returns whether item was actually removed.
	 * Perfect for optimistic deletions with SQLite confirmation.
	 *
	 * @param id The ID of the record to remove
	 * @returns true if item was removed, false if it didn't exist
	 *
	 * @example
	 * // Optimistic deletion pattern:
	 * // 1. Immediate optimistic removal
	 * const wasRemoved = userTable.safeRemove(userId);
	 *
	 * // 2. Send to worker (your code)
	 * worker.postMessage({ type: 'delete', id: userId });
	 *
	 * // 3. Worker confirms (your code handles this)
	 * worker.onmessage = (e) => {
	 *   if (e.data.type === 'delete_confirmed') {
	 *     userTable.safeRemove(e.data.id); // Idempotent - no error
	 *   }
	 * };
	 */
	safeRemove(id: any): boolean {
		const item = this.data.get(id);
		if (!item) {
			return false; // Item not found - no-op
		}

		this.data.delete(id);

		// Create delta and notify subscribers
		const delta = new ZSet([[item, -1]]);
		this.notifySubscribers(delta);

		return true;
	}

	/**
	 * Apply a raw ZSet delta directly to the table. Advanced operation for conflict resolution.
	 *
	 * Use cases:
	 * - Initial data loading from SQLite (massive positive-weight ZSet)
	 * - Conflict resolution when optimistic updates were incorrect
	 * - Batch corrections from SQLite worker
	 *
	 * @param delta ZSet containing weighted changes to apply
	 *
	 * @example
	 * // Initial data loading from SQLite worker:
	 * worker.onmessage = (e) => {
	 *   if (e.data.type === 'initial_data') {
	 *     const initialZSet = new ZSet(e.data.items.map(item => [item, 1]));
	 *     userTable.applyDelta(initialZSet);
	 *   }
	 * };
	 *
	 * @example
	 * // Conflict resolution - correct an optimistic update:
	 * worker.onmessage = (e) => {
	 *   if (e.data.type === 'conflict_resolution') {
	 *     // Remove incorrect optimistic item, add correct one
	 *     const correctionDelta = new ZSet([
	 *       [e.data.incorrectItem, -1],
	 *       [e.data.correctItem, 1]
	 *     ]);
	 *     userTable.applyDelta(correctionDelta);
	 *   }
	 * };
	 */
	applyDelta(delta: ZSet<T>): void {
		// Apply each change in the delta
		for (const [item, weight] of delta.data) {
			const id = item[this.idField];

			if (weight > 0) {
				// Add/update items with positive weight
				this.data.set(id, item);
			} else if (weight < 0) {
				// Remove items with negative weight
				this.data.delete(id);
			}
			// weight === 0 is no-op
		}

		// Notify subscribers of the delta
		this.notifySubscribers(delta);
	}

	clear(): void {
		const allItems = Array.from(this.data.values());
		this.data.clear();

		if (allItems.length > 0) {
			const clearDelta = new ZSet(allItems.map((item) => [item, -1]));
			this.notifySubscribers(clearDelta);
		}
	}

	/**
	 * Apply multiple operations atomically with a single subscriber notification.
	 *
	 * More efficient than individual calls when you need to apply many changes.
	 * All operations are processed, then subscribers get one combined delta.
	 *
	 * @param operations Array of operations to apply
	 *
	 * @example
	 * // Batch operations from SQLite worker:
	 * userTable.batch([
	 *   { type: 'upsert', item: { id: 1, name: 'Alice' } },
	 *   { type: 'upsert', item: { id: 2, name: 'Bob' } },
	 *   { type: 'remove', id: 3 }
	 * ]);
	 * // Subscribers get one notification with combined delta
	 */
	batch(operations: BatchOperation<T>[]): void {
		const deltas: ZSet<T>[] = [];

		// Process each operation and collect deltas
		for (const op of operations) {
			switch (op.type) {
				case 'upsert': {
					const id = op.item[this.idField];
					const oldItem = this.data.get(id);
					this.data.set(id, op.item);

					if (oldItem) {
						deltas.push(
							new ZSet([
								[oldItem, -1],
								[op.item, 1]
							])
						);
					} else {
						deltas.push(new ZSet([[op.item, 1]]));
					}
					break;
				}
				case 'remove': {
					const item = this.data.get(op.id);
					if (item) {
						this.data.delete(op.id);
						deltas.push(new ZSet([[item, -1]]));
					}
					break;
				}
				case 'delta': {
					// Apply delta changes to internal data
					for (const [item, weight] of op.delta.data) {
						const id = item[this.idField];
						if (weight > 0) {
							this.data.set(id, item);
						} else if (weight < 0) {
							this.data.delete(id);
						}
					}
					deltas.push(op.delta);
					break;
				}
			}
		}

		// Combine all deltas and notify once
		if (deltas.length > 0) {
			const group = new ZSetGroup<T>();
			const combinedDelta = deltas.reduce((acc, delta) => group.add(acc, delta));
			this.notifySubscribers(combinedDelta);
		}
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

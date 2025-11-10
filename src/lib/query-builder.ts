import { ZSet, ZSetGroup } from './z-set.js';
import { StatefulJoinCircuit, createStatefulJoinCircuit } from './stateful-circuit.js';
import { StatefulTopK } from './stateful-top-k.js';
import type { ReactiveTable } from './reactive-table.js';

// Internal interface for query execution nodes
interface QueryNode<T> {
	execute(): ZSet<T>;
	processIncrement(delta: ZSet<any>): ZSet<T>;
	subscribe(callback: (delta: ZSet<T>) => void): () => void;
}

/**
 * Main QueryBuilder class for fluent API
 */
export class QueryBuilder<T> {
	constructor(private node: QueryNode<T>) {}

	/**
	 * Join with another table
	 */
	join<U extends Record<string, any>, K>(
		other: ReactiveTable<U>,
		thisKey: (item: T) => K,
		otherKey: (item: U) => K
	): JoinedQueryBuilder<T, U> {
		const joinNode = new JoinNode(this.node, other, thisKey, otherKey);
		return new JoinedQueryBuilder(joinNode);
	}

	/**
	 * Filter records
	 */
	where(predicate: (item: T) => boolean): QueryBuilder<T> {
		const filterNode = new FilterNode(this.node, predicate);
		return new QueryBuilder(filterNode);
	}

	/**
	 * Sort records
	 */
	sortBy(
		comparatorOrField: ((a: T, b: T) => number) | keyof T,
		direction: 'asc' | 'desc' = 'asc'
	): QueryBuilder<T> {
		const comparator = this.createComparator(comparatorOrField, direction);
		const sortNode = new SortNode(this.node, comparator);
		return new QueryBuilder(sortNode);
	}

	/**
	 * Limit number of results
	 */
	limit(count: number, offset: number = 0): QueryBuilder<T> {
		const limitNode = new LimitNode(this.node, count, offset);
		return new QueryBuilder(limitNode);
	}

	/**
	 * Get reactive store that updates automatically
	 */
	reactive(): ReactiveQueryResult<T> {
		return new ReactiveQueryResult(this.node);
	}

	/**
	 * Get current snapshot (non-reactive)
	 */
	toArray(): T[] {
		const zset = this.node.execute();
		return zset.data.filter(([_, weight]) => weight > 0).map(([record, _]) => record);
	}

	private createComparator<T>(
		comparatorOrField: ((a: T, b: T) => number) | keyof T,
		direction: 'asc' | 'desc'
	): (a: T, b: T) => number {
		if (typeof comparatorOrField === 'function') {
			return direction === 'desc' ? (a, b) => comparatorOrField(b, a) : comparatorOrField;
		}

		// Field-based comparator
		const field = comparatorOrField as keyof T;
		return (a: T, b: T) => {
			const aVal = a[field];
			const bVal = b[field];
			let result = 0;

			if (aVal < bVal) result = -1;
			else if (aVal > bVal) result = 1;

			return direction === 'desc' ? -result : result;
		};
	}
}

/**
 * Extended query builder for joined data
 */
export class JoinedQueryBuilder<T, U> {
	constructor(private node: QueryNode<[T, U]>) {}

	/**
	 * Project joined data to new format
	 */
	select<R>(projection: (left: T, right: U) => R): QueryBuilder<R> {
		const selectNode = new TupleSelectNode(this.node, projection);
		return new QueryBuilder(selectNode);
	}

	/**
	 * Filter joined records
	 */
	where(predicate: (left: T, right: U) => boolean): JoinedQueryBuilder<T, U> {
		const filterNode = new FilterNode<[T, U]>(this.node, ([left, right]: [T, U]) =>
			predicate(left, right)
		);
		return new JoinedQueryBuilder(filterNode);
	}

	/**
	 * Get reactive store that updates automatically
	 */
	reactive(): ReactiveQueryResult<[T, U]> {
		return new ReactiveQueryResult(this.node);
	}

	/**
	 * Get current snapshot (non-reactive)
	 */
	toArray(): [T, U][] {
		const zset = this.node.execute();
		return zset.data.filter(([_, weight]) => weight > 0).map(([record, _]) => record);
	}
}

/**
 * Reactive query result that acts like a Svelte store
 */
export class ReactiveQueryResult<T> {
	private subscribers = new Set<(value: T[]) => void>();
	private currentValue: T[] = [];

	constructor(private node: QueryNode<T>) {
		// Initialize current value
		this.updateCurrentValue();

		// Subscribe to changes
		this.node.subscribe((delta) => {
			this.updateCurrentValue();
			this.notifySubscribers();
		});
	}

	/**
	 * Subscribe to changes (Svelte store compatible)
	 */
	subscribe(callback: (value: T[]) => void): () => void {
		this.subscribers.add(callback);
		callback(this.currentValue); // Call immediately with current value
		return () => this.subscribers.delete(callback);
	}

	private updateCurrentValue(): void {
		const zset = this.node.execute();
		this.currentValue = zset.data.filter(([_, weight]) => weight > 0).map(([record, _]) => record);
	}

	private notifySubscribers(): void {
		for (const callback of this.subscribers) {
			callback(this.currentValue);
		}
	}
}

// Internal query execution nodes

class TableNode<T extends Record<string, any>> implements QueryNode<T> {
	private unsubscribe?: () => void;
	private subscribers = new Set<(delta: ZSet<T>) => void>();

	constructor(private table: ReactiveTable<T>) {
		this.unsubscribe = table.subscribe((delta) => {
			for (const callback of this.subscribers) {
				callback(delta);
			}
		});
	}

	execute(): ZSet<T> {
		return this.table.toZSet();
	}

	processIncrement(delta: ZSet<T>): ZSet<T> {
		return delta; // Pass through table deltas
	}

	subscribe(callback: (delta: ZSet<T>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}
}

class JoinNode<T, U extends Record<string, any>, K> implements QueryNode<[T, U]> {
	private circuit: StatefulJoinCircuit<T, U, K>;
	private subscribers = new Set<(delta: ZSet<[T, U]>) => void>();

	constructor(
		private leftNode: QueryNode<T>,
		private rightTable: ReactiveTable<U>,
		private leftKey: (item: T) => K,
		private rightKey: (item: U) => K
	) {
		this.circuit = createStatefulJoinCircuit(this.leftKey, this.rightKey);

		// Initialize with current data
		const initialLeft = leftNode.execute();
		const initialRight = rightTable.toZSet();
		this.circuit.initialize(initialLeft, initialRight);

		// Subscribe to left changes
		leftNode.subscribe((leftDelta) => {
			const joinDelta = this.circuit.processIncrement(leftDelta, new ZSet([]));
			this.notifySubscribers(joinDelta);
		});

		// Subscribe to right changes
		rightTable.subscribe((rightDelta) => {
			const joinDelta = this.circuit.processIncrement(new ZSet([]), rightDelta);
			this.notifySubscribers(joinDelta);
		});
	}

	execute(): ZSet<[T, U]> {
		return this.circuit.getMaterializedView();
	}

	processIncrement(delta: ZSet<any>): ZSet<[T, U]> {
		// This shouldn't be called directly on join nodes
		throw new Error('JoinNode.processIncrement should not be called directly');
	}

	subscribe(callback: (delta: ZSet<[T, U]>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	private notifySubscribers(delta: ZSet<[T, U]>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

class SelectNode<T, R> implements QueryNode<R> {
	private subscribers = new Set<(delta: ZSet<R>) => void>();

	constructor(
		private sourceNode: QueryNode<T>,
		private projection: (item: T) => R
	) {
		sourceNode.subscribe((delta) => {
			const projectedDelta = this.projectDelta(delta);
			this.notifySubscribers(projectedDelta);
		});
	}

	execute(): ZSet<R> {
		const sourceZSet = this.sourceNode.execute();
		return this.projectZSet(sourceZSet);
	}

	processIncrement(delta: ZSet<T>): ZSet<R> {
		return this.projectDelta(delta);
	}

	subscribe(callback: (delta: ZSet<R>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	private projectZSet(zset: ZSet<T>): ZSet<R> {
		const projectedData = zset.data.map(
			([record, weight]) => [this.projection(record), weight] as [R, number]
		);
		return new ZSet(projectedData);
	}

	private projectDelta(delta: ZSet<T>): ZSet<R> {
		return this.projectZSet(delta);
	}

	private notifySubscribers(delta: ZSet<R>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

class TupleSelectNode<T, U, R> implements QueryNode<R> {
	private subscribers = new Set<(delta: ZSet<R>) => void>();

	constructor(
		private sourceNode: QueryNode<[T, U]>,
		private projection: (left: T, right: U) => R
	) {
		sourceNode.subscribe((delta) => {
			const projectedDelta = this.projectDelta(delta);
			this.notifySubscribers(projectedDelta);
		});
	}

	execute(): ZSet<R> {
		const sourceZSet = this.sourceNode.execute();
		return this.projectZSet(sourceZSet);
	}

	processIncrement(delta: ZSet<[T, U]>): ZSet<R> {
		return this.projectDelta(delta);
	}

	subscribe(callback: (delta: ZSet<R>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	private projectZSet(zset: ZSet<[T, U]>): ZSet<R> {
		const projectedData = zset.data.map(
			([[left, right], weight]) => [this.projection(left, right), weight] as [R, number]
		);
		return new ZSet(projectedData);
	}

	private projectDelta(delta: ZSet<[T, U]>): ZSet<R> {
		return this.projectZSet(delta);
	}

	private notifySubscribers(delta: ZSet<R>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

class FilterNode<T> implements QueryNode<T> {
	private subscribers = new Set<(delta: ZSet<T>) => void>();

	constructor(
		private sourceNode: QueryNode<T>,
		private predicate: (item: T) => boolean
	) {
		sourceNode.subscribe((delta) => {
			const filteredDelta = this.filterDelta(delta);
			this.notifySubscribers(filteredDelta);
		});
	}

	execute(): ZSet<T> {
		const sourceZSet = this.sourceNode.execute();
		return this.filterZSet(sourceZSet);
	}

	processIncrement(delta: ZSet<T>): ZSet<T> {
		return this.filterDelta(delta);
	}

	subscribe(callback: (delta: ZSet<T>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	private filterZSet(zset: ZSet<T>): ZSet<T> {
		const filteredData = zset.data.filter(([record, _]) => this.predicate(record));
		return new ZSet(filteredData);
	}

	private filterDelta(delta: ZSet<T>): ZSet<T> {
		return this.filterZSet(delta);
	}

	private notifySubscribers(delta: ZSet<T>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

class SortNode<T> implements QueryNode<T> {
	private sortCircuit: StatefulTopK<T>;
	private subscribers = new Set<(delta: ZSet<T>) => void>();

	constructor(
		private sourceNode: QueryNode<T>,
		private comparator: (a: T, b: T) => number
	) {
		this.sortCircuit = new StatefulTopK(comparator, Infinity, 0, new ZSetGroup<T>());

		// Initialize with current data
		const initialData = sourceNode.execute();
		this.sortCircuit.processIncrement(initialData);

		// Subscribe to changes
		sourceNode.subscribe((delta) => {
			const sortedDelta = this.sortCircuit.processIncrement(delta);
			this.notifySubscribers(sortedDelta);
		});
	}

	execute(): ZSet<T> {
		return this.sortCircuit.getCurrentState().topK;
	}

	processIncrement(delta: ZSet<T>): ZSet<T> {
		return this.sortCircuit.processIncrement(delta);
	}

	subscribe(callback: (delta: ZSet<T>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	private notifySubscribers(delta: ZSet<T>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

class LimitNode<T> implements QueryNode<T> {
	private limitCircuit: StatefulTopK<T>;
	private subscribers = new Set<(delta: ZSet<T>) => void>();

	constructor(
		private sourceNode: QueryNode<T>,
		private limit: number,
		private offset: number
	) {
		// Use identity comparator to preserve original order
		this.limitCircuit = new StatefulTopK(
			() => 0, // No sorting, just limiting
			limit,
			offset,
			new ZSetGroup<T>()
		);

		// Initialize with current data
		const initialData = sourceNode.execute();
		this.limitCircuit.processIncrement(initialData);

		// Subscribe to changes
		sourceNode.subscribe((delta) => {
			const limitedDelta = this.limitCircuit.processIncrement(delta);
			this.notifySubscribers(limitedDelta);
		});
	}

	execute(): ZSet<T> {
		return this.limitCircuit.getCurrentState().topK;
	}

	processIncrement(delta: ZSet<T>): ZSet<T> {
		return this.limitCircuit.processIncrement(delta);
	}

	subscribe(callback: (delta: ZSet<T>) => void): () => void {
		this.subscribers.add(callback);
		return () => this.subscribers.delete(callback);
	}

	private notifySubscribers(delta: ZSet<T>): void {
		for (const callback of this.subscribers) {
			callback(delta);
		}
	}
}

/**
 * Create a query from a reactive table
 */
export function createQuery<T extends Record<string, any>>(
	table: ReactiveTable<T>
): QueryBuilder<T> {
	const tableNode = new TableNode(table);
	return new QueryBuilder(tableNode);
}

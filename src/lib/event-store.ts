import { Stream, integrate, ZSet } from './stream.js';

type TodoCreatedEvent = {
	type: 'TodoCreated';
	id: string;
	title: string;
	timestamp: number;
};
type TodoToggledEvent = {
	type: 'TodoToggled';
	id: string;
	timestamp: number;
};
type TodoDeletedEvent = {
	type: 'TodoDeleted';
	id: string;
	timestamp: number;
};

type Todo = {
	id: string;
	title: string;
	done: boolean;
};

export type Event = TodoCreatedEvent | TodoToggledEvent | TodoDeletedEvent;

export function todoCreatedEvent({ title }: { title: string }): TodoCreatedEvent {
	return { type: 'TodoCreated', id: crypto.randomUUID(), title, timestamp: Date.now() };
}

export function todoToggledEvent({ id }: { id: string }): TodoToggledEvent {
	return { type: 'TodoToggled', id, timestamp: Date.now() };
}

export function todoDeletedEvent({ id }: { id: string }): TodoDeletedEvent {
	return { type: 'TodoDeleted', id, timestamp: Date.now() };
}

export class EventStore {
	// Current state (always up-to-date)
	private currentSnapshot = new ZSet();

	// Bounded history for recent queries
	// NOTE: this is an optimization and we lose time-travel and
	// being able trun from the beginning :shrug:
	private recentSnapshots: ZSet[] = [];
	private recentEvents: Event[] = [];

	// Configuration
	private maxHistorySize = 100; // Keep last 100 states
	private maxEventLogSize = 1000; // Keep last 1000 events

	// Full event log (for complete replay if needed)
	private events: Event[] = [];
	private eventCount = 0;

	private subscribers: ((snapshot: ZSet) => void)[] = [];

	subscribe(callback: (snapshot: ZSet) => void) {
		this.subscribers.push(callback);
		// Return unsubscribe function
		return () => {
			const index = this.subscribers.indexOf(callback);
			if (index > -1) this.subscribers.splice(index, 1);
		};
	}

	private notify() {
		const snapshot = this.getCurrentSnapshot();
		this.subscribers.forEach((callback) => callback(snapshot));
	}

	append(event: Event) {
		const startTime = performance.now();

		// Add to full event log
		this.events.push(event);
		this.eventCount++;

		// Store current snapshot in recent history before updating
		this.recentSnapshots.push(this.currentSnapshot);
		this.recentEvents.push(event);

		// Maintain bounded history
		if (this.recentSnapshots.length > this.maxHistorySize) {
			this.recentSnapshots.shift();
			this.recentEvents.shift();
		}

		// Apply change to current state (DBSP core)
		const change = eventToZSetChange(event, this.currentSnapshot);
		this.currentSnapshot = this.currentSnapshot.plus(change);

		this.notify();

		if (window.debug) {
			console.log(
				`⚡ Event ${this.eventCount} processed in ${(performance.now() - startTime).toFixed(2)}ms`
			);
			console.log(
				`📊 Current: ${this.currentSnapshot.materialize.length} todos, History: ${this.recentSnapshots.length} snapshots`
			);
		}
	}

	getCurrentSnapshot(): ZSet {
		return this.currentSnapshot;
	}

	getTodos(): Todo[] {
		return this.currentSnapshot.materialize;
	}

	getSnapshotAt(stepsBack: number): ZSet | null {
		if (stepsBack === 0) return this.currentSnapshot;
		const index = this.recentSnapshots.length - stepsBack;
		return index >= 0 ? this.recentSnapshots[index] : null;
	}

	getRecentEvents(count: number = 10): Event[] {
		return this.recentEvents.slice(-count);
	}

	getZSetDebug(): Map<string, number> {
		return this.getCurrentSnapshot().debug();
	}
}

// New function in event-store.ts
export function eventToZSetChange(event: Event, currentSnapshot?: ZSet): ZSet {
	if (window.debug) {
		console.log(
			`🔄 Processing event: ${event.type}, snapshot size: ${currentSnapshot?.materialize.length || 0}`
		);
	}
	const change = new ZSet();

	if (event.type === 'TodoCreated') {
		const todo = { id: event.id, title: event.title, done: false };
		change.add(todo, 1);
	} else if (event.type === 'TodoToggled') {
		// Need current snapshot to know what to toggle
		if (currentSnapshot) {
			// Now uses O(1) lookups instead of O(k) scans
			const todo = currentSnapshot?.findById(event.id);
			if (todo) {
				change.add(todo, -1); // Remove old
				change.add({ ...todo, done: !todo.done }, 1); // Add new
			}
		}
	} else if (event.type === 'TodoDeleted') {
		if (currentSnapshot) {
			const todo = currentSnapshot?.findById(event.id);
			if (todo) {
				change.add(todo, -1); // Remove the todo
			}
		}
	}

	if (window.debug) {
		console.log(`✅ Generated change with ${change.materialize.length} items`);
	}
	return change;
}

export function completedTodosCount(todosSnapshot: ZSet): number {
	return todosSnapshot.materialize.filter((todo) => todo.done).length;
}

export function activeTodos(todosSnapshot: ZSet): Todo[] {
	return todosSnapshot.materialize.filter((todo) => !todo.done);
}

export function todoStats(todosSnapshot: ZSet): {
	total: number;
	completed: number;
	active: number;
	completionRate: number;
} {
	console.log(`📈 Computing stats for ${todosSnapshot.materialize.length} todos`);
	const todos = todosSnapshot.materialize;
	const completed = todos.filter((t) => t.done).length;
	const total = todos.length;
	return {
		total,
		completed,
		active: total - completed,
		completionRate: total > 0 ? completed / total : 0
	};
}

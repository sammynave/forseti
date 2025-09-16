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
	events: Event[] = [];
	private eventsStream = new Stream<Event>();
	private changesStream = new Stream<ZSet>();
	private snapshotsStream = new Stream<ZSet>();
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

		this.events.push(event);
		const t = this.eventsStream.append(event);

		// Get previous snapshot (O(1))
		const currentSnapshot = t > 0 ? this.snapshotsStream.get(t - 1) : new ZSet();

		// Convert event to change (O(1))
		const change = eventToZSetChange(event, currentSnapshot);
		this.changesStream.set(t, change);

		// ✅ EFFICIENT: Incremental update (O(|change|))
		const newSnapshot = currentSnapshot.plus(change);
		this.snapshotsStream.set(t, newSnapshot);
		this.notify(); // Notify subscribers after state change
		const endTime = performance.now();
		if (window.debug) {
			console.log(`⚡ Event processed in ${(endTime - startTime).toFixed(2)}ms`);
			console.log(
				`📊 Total events: ${this.events.length}, DB size: ${newSnapshot.materialize.length}`
			);
		}
	}

	getTodos(): Todo[] {
		const latestTime = this.snapshotsStream.length - 1;
		if (latestTime >= 0) {
			return this.snapshotsStream.get(latestTime).materialize;
		}
		return [];
	}

	getCurrentSnapshot(): ZSet {
		const latestTime = this.snapshotsStream.length - 1;
		if (latestTime >= 0) {
			return this.snapshotsStream.get(latestTime);
		}
		return new ZSet();
	}

	getZSetDebug(): Map<string, number> {
		const latestTime = this.snapshotsStream.length - 1;
		if (latestTime >= 0) {
			return this.snapshotsStream.get(latestTime).debug();
		}
		return new Map();
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
			const currentTodos = currentSnapshot.materialize;
			const todo = currentTodos.find((t: Todo) => t.id === event.id);
			if (todo) {
				change.add(todo, -1); // Remove old
				change.add({ ...todo, done: !todo.done }, 1); // Add new
			}
		}
	} else if (event.type === 'TodoDeleted') {
		if (currentSnapshot) {
			const currentTodos = currentSnapshot.materialize;
			const todo = currentTodos.find((t: Todo) => t.id === event.id);
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

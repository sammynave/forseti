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

	append(event: Event) {
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
	}

	getTodos(): Todo[] {
		const latestTime = this.snapshotsStream.length - 1;
		if (latestTime >= 0) {
			return this.snapshotsStream.get(latestTime).materialize;
		}
		return [];
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

	return change;
}

// Then refactor EventStore to use streams + integration

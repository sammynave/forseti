import { ZSet } from './stream.js';

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
	private todos = new ZSet();

	append(event: Event) {
		this.events.push(event);
		this.applyEventToZSet(event);
	}

	private applyEventToZSet(event: TodoCreatedEvent | TodoToggledEvent | TodoDeletedEvent) {
		if (event.type === 'TodoCreated') {
			this.insert({ id: event.id, title: event.title, done: false }, 'todos');
		} else if (event.type === 'TodoToggled') {
			// Find the current todo and toggle it
			const currentTodos = this.todos.materialize;
			const todo = currentTodos.find((t) => t.id === event.id);
			if (todo) {
				this.delete(todo, 'todos'); // Remove old version (weight -1)
				this.insert({ ...todo, done: !todo.done }, 'todos'); // Add new version (weight +1)
			}
		} else if (event.type === 'TodoDeleted') {
			const currentTodos = this.todos.materialize;
			const todo = currentTodos.find((t) => t.id === event.id);
			if (todo) {
				this.delete(todo, 'todos');
			}
		}
	}

	/* STAART Maybe these should move */
	// Convenience methods for common operations
	insert(item: unknown, into: 'todos') {
		this[into].add(item, 1);
	}

	delete(item: unknown, into: 'todos') {
		this[into].add(item, -1);
	}

	getTodos(): Todo[] {
		return this.todos.materialize;
	}

	getZSetDebug(): Map<string, number> {
		return this.todos.debug();
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
		// Similar logic for delete
	}

	return change;
}

// Then refactor EventStore to use streams + integration

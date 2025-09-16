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

type Event = TodoCreatedEvent | TodoToggledEvent | TodoDeletedEvent;

export function todoCreatedEvent({ title }: { title: string }): TodoCreatedEvent {
	return { type: 'TodoCreated', id: crypto.randomUUID(), title, timestamp: Date.now() };
}

export function todoToggledEvent({ id }: { id: string }): TodoToggledEvent {
	return { type: 'TodoToggled', id, timestamp: Date.now() };
}

export function deleteToggledEvent({ id }: { id: string }): TodoDeletedEvent {
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
			const currentTodos = this.todos.materialize();
			const todo = currentTodos.find((t) => t.id === event.id);
			if (todo) {
				this.delete(todo, 'todos'); // Remove old version (weight -1)
				this.insert({ ...todo, done: !todo.done }, 'todos'); // Add new version (weight +1)
			}
		} else if (event.type === 'TodoDeleted') {
			const currentTodos = this.todos.materialize();
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
		return this.todos.materialize();
	}

	getZSetDebug(): Map<string, number> {
		return this.todos.debug();
	}
}

class ZSet {
	private data = new Map<string, number>();

	// DBSP-style: add element with specified weight (default +1 for insertion)
	add(item: unknown, weight: number = 1) {
		const key = JSON.stringify(item);
		const currentWeight = this.data.get(key) || 0;
		const newWeight = currentWeight + weight;

		// DBSP semantics: keep all weights, including 0
		this.data.set(key, newWeight);
	}
	// Convert to regular Todo array for display (only positive weights)
	materialize(): Todo[] {
		return Array.from(this.data.entries())
			.filter(([_, weight]) => weight > 0)
			.map(([todoJson, _]) => JSON.parse(todoJson));
	}
	/* END Maybe these should move */

	// Debug: show the raw Z-set data
	debug(): Map<string, number> {
		return new Map(this.data);
	}
}

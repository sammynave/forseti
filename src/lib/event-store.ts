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

type Event = TodoCreatedEvent | TodoToggledEvent;

export class EventStore {
	events: Event[] = [];

	append(event: Event) {
		this.events.push(event);
	}
}

export function todoCreatedEvent({ title }: { title: string }): TodoCreatedEvent {
	return { type: 'TodoCreated', id: crypto.randomUUID(), title, timestamp: Date.now() };
}

export function todoToggledEvent({ id }: { id: string }): TodoToggledEvent {
	return { type: 'TodoToggled', id, timestamp: Date.now() };
}

export function rebuildTodosFromEvents(events: Event[]) {
	const todos: { id: string; title: string; done: boolean }[] = [];

	for (const event of events) {
		switch (event.type) {
			case 'TodoCreated':
				todos.push({
					id: event.id,
					title: event.title,
					done: false
				});
				break;

			case 'TodoToggled':
				const todo = todos.find((t) => t.id === event.id);
				if (todo) {
					todo.done = !todo.done;
				}
				break;
		}
	}

	return todos;
}

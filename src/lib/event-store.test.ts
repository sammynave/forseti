import { describe, expect, it } from 'vitest';
import {
	EventStore,
	eventToZSetChange,
	todoCreatedEvent,
	todoDeletedEvent,
	todoToggledEvent
} from './event-store.js';
import { ZSet } from './stream.js';

describe('eventToZSetChange', () => {
	it('converts TodoCreated to Z-set change', () => {
		const event = todoCreatedEvent({ title: 'Test Todo' });
		const change = eventToZSetChange(event);

		const materialized = change.materialize;
		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual({
			id: event.id,
			title: 'Test Todo',
			done: false
		});
	});

	it('converts TodoToggled to Z-set change', () => {
		// Setup: Create current snapshot with a todo
		const currentSnapshot = new ZSet();
		const existingTodo = { id: 'test-id', title: 'Test', done: false };
		currentSnapshot.add(existingTodo, 1);

		const event = todoToggledEvent({ id: 'test-id' });
		const change = eventToZSetChange(event, currentSnapshot);

		// Should remove old todo and add toggled version
		expect(change.debug().get(JSON.stringify(existingTodo))).toBe(-1);
		expect(change.debug().get(JSON.stringify({ ...existingTodo, done: true }))).toBe(1);
	});

	it('converts TodoDeleted to Z-set change', () => {
		// Setup: Create current snapshot with a todo
		const currentSnapshot = new ZSet();
		const existingTodo = { id: 'test-id', title: 'Test', done: false };
		currentSnapshot.add(existingTodo, 1);

		const event = todoDeletedEvent({ id: 'test-id' });
		const change = eventToZSetChange(event, currentSnapshot);

		// Should remove old todo
		expect(change.debug().get(JSON.stringify(existingTodo))).toBe(-1);
	});
});

describe('EventStore', () => {
	it('maintains todos through DBSP streams', () => {
		const store = new EventStore();

		// Create todo
		const createEvent = todoCreatedEvent({ title: 'Test Todo' });
		store.append(createEvent);

		expect(store.getTodos()).toHaveLength(1);
		expect(store.getTodos()[0].title).toBe('Test Todo');
		expect(store.getTodos()[0].done).toBe(false);

		// Toggle todo
		const toggleEvent = todoToggledEvent({ id: createEvent.id });
		store.append(toggleEvent);

		expect(store.getTodos()).toHaveLength(1);
		expect(store.getTodos()[0].done).toBe(true);

		// Delete todo
		const deleteEvent = todoDeletedEvent({ id: createEvent.id });
		store.append(deleteEvent);

		expect(store.getTodos()).toHaveLength(0);
	});
});

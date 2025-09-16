import { describe, expect, it } from 'vitest';
import {
	activeTodos,
	completedTodosCount,
	EventStore,
	eventToZSetChange,
	todoCreatedEvent,
	todoDeletedEvent,
	todoStats,
	todoToggledEvent
} from './event-store.js';
import { ZSet } from './z-set.js';

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

describe('EventStore Methods', () => {
	it('subscribe notifies on changes', () => {
		const store = new EventStore();
		let notificationCount = 0;
		let lastSnapshot = null;

		const unsubscribe = store.subscribe((snapshot) => {
			notificationCount++;
			lastSnapshot = snapshot;
		});

		const event = todoCreatedEvent({ title: 'Test' });
		store.append(event);

		expect(notificationCount).toBe(1);
		expect(lastSnapshot.materialize).toHaveLength(1);

		unsubscribe();
	});

	it('getCurrentSnapshot returns current state', () => {
		const store = new EventStore();
		const event = todoCreatedEvent({ title: 'Test' });
		store.append(event);

		const snapshot = store.getCurrentSnapshot();
		expect(snapshot.materialize).toHaveLength(1);
		expect(snapshot.materialize[0].title).toBe('Test');
	});

	it('getSnapshotAt provides time travel', () => {
		const store = new EventStore();

		// Initial state (empty)
		const event1 = todoCreatedEvent({ title: 'Todo 1' });
		store.append(event1);

		const event2 = todoCreatedEvent({ title: 'Todo 2' });
		store.append(event2);

		// Current state should have 2 todos
		expect(store.getSnapshotAt(0).materialize).toHaveLength(2);

		// 1 step back should have 1 todo
		expect(store.getSnapshotAt(1).materialize).toHaveLength(1);
	});

	it('getRecentEvents returns event history', () => {
		const store = new EventStore();

		const event1 = todoCreatedEvent({ title: 'Todo 1' });
		const event2 = todoCreatedEvent({ title: 'Todo 2' });

		store.append(event1);
		store.append(event2);

		const recent = store.getRecentEvents(2);
		expect(recent).toHaveLength(2);
		expect(recent[0].type).toBe('TodoCreated');
		expect(recent[1].type).toBe('TodoCreated');
	});

	it('getZSetDebug returns debug info', () => {
		const store = new EventStore();
		const event = todoCreatedEvent({ title: 'Test' });
		store.append(event);

		const debug = store.getZSetDebug();
		expect(debug instanceof Map).toBe(true);
		expect(debug.size).toBeGreaterThan(0);
	});
});

describe('Event Factory Functions', () => {
	it('todoCreatedEvent creates valid event', () => {
		const event = todoCreatedEvent({ title: 'Test Todo' });

		expect(event.type).toBe('TodoCreated');
		expect(event.title).toBe('Test Todo');
		expect(typeof event.id).toBe('string');
		expect(typeof event.timestamp).toBe('number');
	});

	it('todoToggledEvent creates valid event', () => {
		const event = todoToggledEvent({ id: 'test-id' });

		expect(event.type).toBe('TodoToggled');
		expect(event.id).toBe('test-id');
		expect(typeof event.timestamp).toBe('number');
	});

	it('todoDeletedEvent creates valid event', () => {
		const event = todoDeletedEvent({ id: 'test-id' });

		expect(event.type).toBe('TodoDeleted');
		expect(event.id).toBe('test-id');
		expect(typeof event.timestamp).toBe('number');
	});
});

describe('Utility Functions', () => {
	it('completedTodosCount counts completed todos', () => {
		const snapshot = new ZSet();
		snapshot.add({ id: '1', title: 'Done', done: true }, 1);
		snapshot.add({ id: '2', title: 'Not Done', done: false }, 1);

		expect(completedTodosCount(snapshot)).toBe(1);
	});

	it('activeTodos returns non-completed todos', () => {
		const snapshot = new ZSet();
		const activeTodo = { id: '1', title: 'Active', done: false };
		const completedTodo = { id: '2', title: 'Done', done: true };

		snapshot.add(activeTodo, 1);
		snapshot.add(completedTodo, 1);

		const active = activeTodos(snapshot);
		expect(active).toHaveLength(1);
		expect(active[0]).toEqual(activeTodo);
	});

	it('todoStats computes statistics', () => {
		const snapshot = new ZSet();
		snapshot.add({ id: '1', title: 'Done', done: true }, 1);
		snapshot.add({ id: '2', title: 'Not Done', done: false }, 1);

		const stats = todoStats(snapshot);
		expect(stats.total).toBe(2);
		expect(stats.completed).toBe(1);
		expect(stats.active).toBe(1);
		expect(stats.completionRate).toBe(0.5);
	});
});

describe('eventToZSetChange - Edge Cases', () => {
	it('handles TodoToggled without current snapshot', () => {
		const event = todoToggledEvent({ id: 'non-existent' });
		const change = eventToZSetChange(event); // No snapshot provided

		// Should return empty change since no current state to toggle
		expect(change.materialize).toHaveLength(0);
	});

	it('handles TodoDeleted without current snapshot', () => {
		const event = todoDeletedEvent({ id: 'non-existent' });
		const change = eventToZSetChange(event); // No snapshot provided

		// Should return empty change since nothing to delete
		expect(change.materialize).toHaveLength(0);
	});

	it('handles TodoToggled with non-existent ID', () => {
		const currentSnapshot = new ZSet();
		const existingTodo = { id: 'existing-id', title: 'Test', done: false };
		currentSnapshot.add(existingTodo, 1);

		const event = todoToggledEvent({ id: 'non-existent-id' });
		const change = eventToZSetChange(event, currentSnapshot);

		// Should return empty change since ID doesn't exist
		expect(change.materialize).toHaveLength(0);
	});

	it('handles TodoDeleted with non-existent ID', () => {
		const currentSnapshot = new ZSet();
		const existingTodo = { id: 'existing-id', title: 'Test', done: false };
		currentSnapshot.add(existingTodo, 1);

		const event = todoDeletedEvent({ id: 'non-existent-id' });
		const change = eventToZSetChange(event, currentSnapshot);

		// Should return empty change since ID doesn't exist
		expect(change.materialize).toHaveLength(0);
	});

	it('TodoCreated generates correct todo structure', () => {
		const event = todoCreatedEvent({ title: 'New Todo' });
		const change = eventToZSetChange(event);

		const materialized = change.materialize;
		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual({
			id: event.id,
			title: 'New Todo',
			done: false // Should always start as false
		});
	});

	it('TodoToggled correctly flips done state', () => {
		// Test toggling from false to true
		const currentSnapshot1 = new ZSet();
		const todo1 = { id: 'test-id', title: 'Test', done: false };
		currentSnapshot1.add(todo1, 1);

		const toggleEvent1 = todoToggledEvent({ id: 'test-id' });
		const change1 = eventToZSetChange(toggleEvent1, currentSnapshot1);

		expect(change1.debug().get(JSON.stringify(todo1))).toBe(-1); // Remove old
		expect(change1.debug().get(JSON.stringify({ ...todo1, done: true }))).toBe(1); // Add new

		// Test toggling from true to false
		const currentSnapshot2 = new ZSet();
		const todo2 = { id: 'test-id', title: 'Test', done: true };
		currentSnapshot2.add(todo2, 1);

		const toggleEvent2 = todoToggledEvent({ id: 'test-id' });
		const change2 = eventToZSetChange(toggleEvent2, currentSnapshot2);

		expect(change2.debug().get(JSON.stringify(todo2))).toBe(-1); // Remove old
		expect(change2.debug().get(JSON.stringify({ ...todo2, done: false }))).toBe(1); // Add new
	});
});

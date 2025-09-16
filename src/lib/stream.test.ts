import { describe, expect, it } from 'vitest';
import { todoCreatedEvent, todoDeletedEvent, todoToggledEvent, type Event } from './event-store.js';
import { integrate, Stream, ZSet } from './stream.js';

describe('Stream', () => {
	it('can set and get', () => {
		const eventStream = new Stream<Event>();

		const firstEvent = todoCreatedEvent({ title: 'test' });
		const secondEvent = todoToggledEvent({ id: '123' });

		eventStream.set(0, firstEvent);
		eventStream.set(1, secondEvent);

		expect(eventStream.get(0)).toStrictEqual(firstEvent);
		expect(eventStream.get(1)).toStrictEqual(secondEvent);
		expect(eventStream.length).toBe(2);
		expect(eventStream.isDefined(0)).toBe(true);
		expect(eventStream.isDefined(1)).toBe(true);
		expect(eventStream.isDefined(2)).toBe(false);
		eventStream.append(todoDeletedEvent({ id: '123' }));
		expect(eventStream.isDefined(2)).toBe(true);
		expect(eventStream.length).toBe(3);
	});

	it('integrate() accumulates Z-set changes into snapshots', () => {
		// Create stream of Z-set changes
		const changes = new Stream<ZSet>();

		const todo1 = { id: 'todo1', title: 'Test', done: false };
		const todo2 = { id: 'todo2', title: 'Test2', done: false };

		// t=0: Add todo1
		const change0 = new ZSet();
		change0.add(todo1, 1);
		changes.set(0, change0);

		// t=1: Add todo2, modify todo1
		const change1 = new ZSet();
		change1.add(todo2, 1);
		change1.add(todo1, 2); // Now todo1 has weight 3 total
		changes.set(1, change1);

		// t=2: Delete todo1
		const change2 = new ZSet();
		change2.add(todo1, -3); // Remove all of todo1
		changes.set(2, change2);

		const snapshots = integrate(changes);

		// Check accumulated snapshots
		expect(snapshots.get(0).materialize).toEqual([todo1]); // Just todo1
		expect(snapshots.get(1).materialize).toEqual([todo1, todo2]); // Both todos
		expect(snapshots.get(2).materialize).toEqual([todo2]); // Just todo2
	});
});

describe('ZSet', () => {
	it('can combined two ZSets', () => {
		const zset1 = new ZSet();
		zset1.add('todo1', 1);
		zset1.add('todo2', 2);

		const zset2 = new ZSet();
		zset2.add('todo2', -1); // Reduces weight
		zset2.add('todo3', 1); // New item

		const result = zset1.plus(zset2);
		expect(Array.from(result.data.entries())).toStrictEqual([
			['"todo1"', 1],
			['"todo2"', 1],
			['"todo3"', 1]
		]);
	});

	it('is commutative', () => {
		const zset1 = new ZSet();
		zset1.add('todo1', 1);
		zset1.add('todo2', 2);

		const zset2 = new ZSet();
		zset2.add('todo2', -1); // Reduces weight
		zset2.add('todo3', 1); // New item

		const result = zset1.plus(zset2);
		const result2 = zset2.plus(zset1);
		expect(result).toStrictEqual(result2);
	});

	it('handles zero weights correctly', () => {
		const zset1 = new ZSet();
		zset1.add('item', 5);

		const zset2 = new ZSet();
		zset2.add('item', -5);

		const result = zset1.plus(zset2);
		expect(result.data.get('"item"')).toBe(0); // Should be 0, not undefined
	});

	it('zero() returns empty Z-set', () => {
		const zset = new ZSet();
		const zero = zset.zero();

		expect(zero.data.size).toBe(0);
		expect(Array.from(zero.data.entries())).toStrictEqual([]);
	});

	it('zero() is additive identity (a + 0 = a)', () => {
		const zset = new ZSet();
		zset.add('todo1', 5);
		zset.add('todo2', -2);

		const result = zset.plus(zset.zero());

		expect(Array.from(result.data.entries())).toStrictEqual([
			['"todo1"', 5],
			['"todo2"', -2]
		]);
	});

	it('negate() flips all weights', () => {
		const zset = new ZSet();
		zset.add('todo1', 3);
		zset.add('todo2', -1);
		zset.add('todo3', 0);

		const negated = zset.negate();

		expect(Array.from(negated.data.entries())).toStrictEqual([
			['"todo1"', -3],
			['"todo2"', 1],
			['"todo3"', -0]
		]);
		expect(-0 === 0).toBe(true);
	});

	it('a + (-a) = zero (additive inverse property)', () => {
		const zset = new ZSet();
		zset.add('todo1', 5);
		zset.add('todo2', -2);

		const result = zset.plus(zset.negate());

		// Should be all zeros (empty when materialized)
		expect(Array.from(result.data.entries())).toStrictEqual([
			['"todo1"', 0],
			['"todo2"', 0]
		]);
		expect(result.materialize).toStrictEqual([]);
	});

	it('double negation: -(-a) = a', () => {
		const zset = new ZSet();
		zset.add('todo1', 3);
		zset.add('todo2', -1);

		const doubleNegated = zset.negate().negate();

		expect(Array.from(doubleNegated.data.entries())).toStrictEqual([
			['"todo1"', 3],
			['"todo2"', -1]
		]);
	});
});

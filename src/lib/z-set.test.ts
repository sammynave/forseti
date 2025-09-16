import { describe, expect, it } from 'vitest';
import {
	EventStore,
	eventToZSetChange,
	todoCreatedEvent,
	todoDeletedEvent,
	todoToggledEvent
} from './event-store.js';
import { ZSet } from './z-set.js';

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

describe('ZSet', () => {
	it('findById returns correct item', () => {
		const todo1 = { id: '123', title: 'test', done: false };
		const todo2 = { id: '456', title: 'test 2', done: true };
		const z = new ZSet();
		z.add(todo1, 1);
		z.add(todo2, 1);
		expect(z.findById(todo1.id)).toStrictEqual(todo1);
		expect(z.findById('890')).toBeUndefined;
	});

	it('add works with default and custom weights', () => {
		const zset = new ZSet();
		const item1 = { id: 'test-1', name: 'Item 1' };
		const item2 = { id: 'test-2', name: 'Item 2' };

		zset.add(item1); // default weight 1
		zset.add(item2, 3); // custom weight 3

		const materialized = zset.materialize;
		expect(materialized).toHaveLength(2);
		expect(materialized).toContainEqual(item1);
		expect(materialized).toContainEqual(item2);
	});

	it('plus combines two ZSets', () => {
		const zset1 = new ZSet();
		const zset2 = new ZSet();
		const item1 = { id: 'test-1', name: 'Item 1' };
		const item2 = { id: 'test-2', name: 'Item 2' };

		zset1.add(item1);
		zset2.add(item2);

		const result = zset1.plus(zset2);
		const materialized = result.materialize;

		expect(materialized).toHaveLength(2);
		expect(materialized).toContainEqual(item1);
		expect(materialized).toContainEqual(item2);
	});

	it('zero returns empty ZSet', () => {
		const zset = new ZSet();
		const empty = zset.zero();

		expect(empty.materialize).toHaveLength(0);
	});

	it('negate inverts all weights', () => {
		const zset = new ZSet();
		const item = { id: 'test-1', name: 'Item 1' };
		zset.add(item, 2);

		const negated = zset.negate();

		// Negative weights don't appear in materialize
		expect(negated.materialize).toHaveLength(0);
		expect(negated.data).toStrictEqual(new Map([['{"id":"test-1","name":"Item 1"}', -2]]));
	});

	it('materialize returns only positive weight items', () => {
		const zset = new ZSet();
		const item1 = { id: 'test-1', name: 'Item 1' };
		const item2 = { id: 'test-2', name: 'Item 2' };

		zset.add(item1, 1);
		zset.add(item2, -1); // negative weight

		const materialized = zset.materialize;
		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual(item1);
		expect(zset.data).toHaveLength(2);
	});

	it('filter applies predicate correctly', () => {
		const zset = new ZSet();
		const item1 = { id: 'test-1', name: 'Keep me' };
		const item2 = { id: 'test-2', name: 'Remove me' };

		zset.add(item1);
		zset.add(item2);

		const filtered = zset.filter((item) => item.name.includes('Keep'));
		const materialized = filtered.materialize;

		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual(item1);
	});

	it('project transforms items', () => {
		const zset = new ZSet();
		const item1 = { id: 'test-1', name: 'Item 1', value: 10 };
		const item2 = { id: 'test-2', name: 'Item 2', value: 20 };

		zset.add(item1);
		zset.add(item2);

		const projected = zset.project((item) => ({ name: item.name }));
		const materialized = projected.materialize;

		expect(materialized).toHaveLength(2);
		expect(materialized).toContainEqual({ name: 'Item 1' });
		expect(materialized).toContainEqual({ name: 'Item 2' });
	});

	it('cartesianProduct creates pairs', () => {
		const zset1 = new ZSet();
		const zset2 = new ZSet();

		zset1.add('A');
		zset1.add('B');
		zset2.add('1');
		zset2.add('2');

		const product = zset1.cartesianProduct(zset2);
		const materialized = product.materialize;

		expect(materialized).toHaveLength(4);
		expect(materialized).toEqual([
			['A', '1'],
			['A', '2'],
			['B', '1'],
			['B', '2']
		]);
	});

	it('distinct converts weights to 1', () => {
		const zset = new ZSet();
		const item = { id: 'test-1', name: 'Item 1' };

		zset.add(item, 5); // weight 5

		const distinct = zset.distinct();
		const materialized = distinct.materialize;

		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual(item);
		expect(distinct.data).toStrictEqual(new Map([['{"id":"test-1","name":"Item 1"}', 1]]));
	});

	it('join combines items with matching keys', () => {
		const zset1 = new ZSet();
		const zset2 = new ZSet();
		const item1 = { id: 'test-1', category: 'A' };
		const item2 = { id: 'test-2', category: 'A' };

		zset1.add(item1);
		zset2.add(item2);

		const joined = zset1.join(
			zset2,
			(item) => item.category,
			(item) => item.category
		);
		const materialized = joined.materialize;

		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual([item1, item2]);
	});

	it('difference removes common items', () => {
		const zset1 = new ZSet();
		const zset2 = new ZSet();
		const item1 = { id: 'test-1', name: 'Item 1' };
		const item2 = { id: 'test-2', name: 'Item 2' };

		zset1.add(item1);
		zset1.add(item2);
		zset2.add(item2); // common item

		const diff = zset1.difference(zset2);
		const materialized = diff.materialize;

		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual(item1);
	});

	it('intersection keeps only common items', () => {
		const zset1 = new ZSet();
		const zset2 = new ZSet();
		const item1 = { id: 'test-1', name: 'Item 1' };
		const item2 = { id: 'test-2', name: 'Item 2' };

		zset1.add(item1);
		zset1.add(item2);
		zset2.add(item2); // common item

		const intersection = zset1.intersection(zset2);
		const materialized = intersection.materialize;

		expect(materialized).toHaveLength(1);
		expect(materialized[0]).toEqual(item2);
	});
});

describe('ZSet older tests - remove dupes someday', () => {
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

import { describe, expect, it } from 'vitest';
import { todoCreatedEvent, todoDeletedEvent, todoToggledEvent, type Event } from './event-store.js';
import { Stream } from './stream.js';
import { integrate } from './stream/utils.js';
import { ZSet } from './z-set.js';

describe('Stream', () => {
	it('can set and get', () => {
		const eventStream = new Stream();

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
		const changes = new Stream();

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

describe('Stream Operators - DBSP Compliance', () => {
	it('differentiate computes stream differences', () => {
		const stream = new Stream();

		// t=0: Add item
		const zset0 = new ZSet();
		zset0.add('item1', 1);
		stream.append(zset0);

		// t=1: Add another item
		const zset1 = new ZSet();
		zset1.add('item1', 1);
		zset1.add('item2', 1);
		stream.append(zset1);

		const diff = stream.differentiate();

		// D[0] = s[0]
		expect(diff.get(0).materialize).toEqual(['item1']);
		// D[1] = s[1] - s[0] = just item2
		// since item1 already existed, it doesn't matter that we add it again
		expect(diff.get(1).materialize).toEqual(['item2']);
	});

	it('delay shifts stream by one time unit', () => {
		const stream = new Stream();
		const zset = new ZSet();
		zset.add('item', 1);
		stream.append(zset);

		const delayed = stream.delay();

		expect(stream.get(0).materialize).toEqual(['item']);
		// First element should be empty
		expect(delayed.get(0).materialize).toEqual([]);
		// Second element should be original first
		expect(delayed.get(1).materialize).toEqual(['item']);
	});

	it('applyIncremental applies incremental query', () => {
		const changes = new Stream();
		const change = new ZSet();
		change.add({ name: 'test' }, 1);
		changes.append(change);

		const result = changes.applyIncremental((stream) =>
			stream.liftFilter((item) => item.name === 'test')
		);

		expect(result.get(0).materialize).toEqual([{ name: 'test' }]);
	});
});

describe('Stream Lifted Operators', () => {
	it('liftFilter filters items in stream', () => {
		const stream = new Stream();
		const zset = new ZSet();
		zset.add({ type: 'keep' }, 1);
		zset.add({ type: 'remove' }, 1);
		stream.append(zset);

		const filtered = stream.liftFilter((item) => item.type === 'keep');

		expect(filtered.get(0).materialize).toEqual([{ type: 'keep' }]);
	});

	it('liftProject transforms items in stream', () => {
		const stream = new Stream();
		const zset = new ZSet();
		zset.add({ name: 'test', value: 42 }, 1);
		stream.append(zset);

		const projected = stream.liftProject((item) => ({ name: item.name }));

		expect(projected.get(0).materialize).toEqual([{ name: 'test' }]);
	});

	it('liftJoin joins two streams', () => {
		const stream1 = new Stream();
		const stream2 = new Stream();

		const zset1 = new ZSet();
		zset1.add({ id: 1, name: 'A' }, 1);
		stream1.append(zset1);

		const zset2 = new ZSet();
		zset2.add({ id: 1, value: 100 }, 1);
		stream2.append(zset2);

		const joined = stream1.liftJoin(
			stream2,
			(item) => item.id,
			(item) => item.id
		);

		expect(joined.get(0).materialize).toEqual([
			[
				{ id: 1, name: 'A' },
				{ id: 1, value: 100 }
			]
		]);
	});

	it('liftDistinct removes duplicates in stream', () => {
		const stream = new Stream();
		const zset = new ZSet();
		zset.add('item', 3); // weight > 1
		stream.append(zset);

		const distinct = stream.liftDistinct();

		// Should have weight 1 now
		expect(distinct.get(0).materialize).toEqual(['item']);
	});
});

describe('Lifting Operator (↑f)', () => {
	it('handles gaps with empty ZSets', () => {
		const stream = new Stream();

		const zset1 = new ZSet();
		zset1.add('item1', 1);

		stream.set(0, zset1);
		stream.set(2, zset1); // Gap at t=1

		// Gap filled with empty ZSet
		expect(stream.get(1).materialize.length).toBe(0);
		expect(stream.length).toBe(3);
	});

	it('lift works correctly', () => {
		const stream = new Stream();

		const zset = new ZSet();
		zset.add('item', 1);
		stream.append(zset);

		const negated = stream.lift((z) => z.negate());
		expect(negated.get(0).debug().get('"item"')).toBe(-1);
	});
});

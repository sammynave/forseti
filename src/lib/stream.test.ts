import { describe, expect, it } from 'vitest';
import { todoCreatedEvent, todoDeletedEvent, todoToggledEvent, type Event } from './event-store.js';
import { Stream } from './stream.js';
import { integrate } from './stream/utils.js';
import { ZSet } from './z-set.js';
import { streamsEqual, zsetsEqual } from './mathematical-properties.test.js';

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
	it('differentiate computes stream differences correctly (Definition 2.15)', () => {
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

		// DBSP Definition 2.15: D(s)[0] = s[0] exactly
		expect(diff.get(0).materialize).toEqual(['item1']);

		// D(s)[1] = s[1] - s[0]
		// s[1] = {item1: 1, item2: 1}, s[0] = {item1: 1}
		// s[1] - s[0] = {item2: 1} (item1 cancels out)
		expect(diff.get(1).materialize).toEqual(['item2']);
	});

	// NEW TEST: Verify the exact paper definition
	it('differentiate matches DBSP Definition 2.15 exactly', () => {
		const stream = new Stream();

		// Create meaningful test data with non-zero weights
		const data = [
			{ item: 'a', weight: 1 }, // t=0: {a: 1}
			{ item: 'b', weight: 2 }, // t=1: {b: 2}
			{ item: 'c', weight: 3 } // t=2: {c: 3}
		];

		data.forEach(({ item, weight }) => {
			const zset = new ZSet();
			zset.add(item, weight);
			stream.append(zset);
		});

		const diff = stream.differentiate();

		// D(s)[0] = s[0] = {a: 1}
		expect(diff.get(0).debug().get('"a"')).toBe(1);

		// D(s)[1] = s[1] - s[0] = {b: 2} - {a: 1} = {b: 2, a: -1}
		expect(diff.get(1).debug().get('"b"')).toBe(2);
		expect(diff.get(1).debug().get('"a"')).toBe(-1);

		// D(s)[2] = s[2] - s[1] = {c: 3} - {b: 2} = {c: 3, b: -2}
		expect(diff.get(2).debug().get('"c"')).toBe(3);
		expect(diff.get(2).debug().get('"b"')).toBe(-2);
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

describe('Theorem 2.20 - Integration/Differentiation Inversion', () => {
	it('verifies I(D(s)) = s exactly (left inverse)', () => {
		const originalStream = new Stream();

		// Create test stream with varied data
		const data = [
			{ items: ['a'], weights: [1] },
			{ items: ['a', 'b'], weights: [1, 1] },
			{ items: ['b'], weights: [2] }, // Different weight
			{ items: ['c'], weights: [1] }
		];

		data.forEach(({ items, weights }) => {
			const zset = new ZSet();
			items.forEach((item, i) => zset.add(item, weights[i]));
			originalStream.append(zset);
		});

		// Apply D then I: should get back original
		const differentiated = originalStream.differentiate();
		const reintegrated = integrate(differentiated);

		// Verify I(D(s)) = s exactly
		expect(streamsEqual(reintegrated, originalStream)).toBe(true);
	});

	it('verifies D(I(s)) = s exactly (right inverse)', () => {
		const changeStream = new Stream();

		// Create change stream (what integration expects)
		const changes = [
			{ items: ['a'], weights: [1] }, // Add 'a'
			{ items: ['b'], weights: [1] }, // Add 'b'
			{ items: ['a'], weights: [-1] }, // Remove 'a'
			{ items: ['c'], weights: [1] } // Add 'c'
		];

		changes.forEach(({ items, weights }) => {
			const zset = new ZSet();
			items.forEach((item, i) => zset.add(item, weights[i]));
			changeStream.append(zset);
		});

		// Apply I then D: should get back original changes
		const integrated = integrate(changeStream);
		const redifferentiated = integrated.differentiate();

		// Verify D(I(s)) = s exactly
		expect(streamsEqual(redifferentiated, changeStream)).toBe(true);
	});

	it('demonstrates why the fix was critical', () => {
		// This test would FAIL with the old differentiation implementation
		const stream = new Stream();
		const zset = new ZSet();
		zset.add('test_item', 1);
		stream.append(zset);

		const diff = stream.differentiate();
		const reintegrated = integrate(diff);

		// With old implementation: D(s)[0] = s[0] - 0 ≠ s[0]
		// This would cause I(D(s))[0] ≠ s[0], breaking inversion

		// With correct implementation: D(s)[0] = s[0] exactly
		// So I(D(s))[0] = s[0], preserving inversion

		expect(zsetsEqual(reintegrated.get(0), stream.get(0))).toBe(true);
	});
});

describe('Stream Union Operations', () => {
	it('liftUnion combines two streams following DBSP Table 1: UNION = distinct(I1 + I2)', () => {
		// Setup: Create two streams with test data
		const stream1 = new Stream();
		const stream2 = new Stream();

		// Test objects - simple and predictable
		const itemA = { id: 'A', name: 'Item A' };
		const itemB = { id: 'B', name: 'Item B' };
		const itemC = { id: 'C', name: 'Item C' };
		const itemD = { id: 'D', name: 'Item D' };

		// t=0: stream1 has {A, B}, stream2 has {B, C}
		const zset1_t0 = new ZSet();
		zset1_t0.add(itemA, 1);
		zset1_t0.add(itemB, 1);
		stream1.append(zset1_t0);

		const zset2_t0 = new ZSet();
		zset2_t0.add(itemB, 1); // B appears in both - should be deduplicated
		zset2_t0.add(itemC, 1);
		stream2.append(zset2_t0);

		// t=1: stream1 adds {D}, stream2 adds {A}
		const zset1_t1 = new ZSet();
		zset1_t1.add(itemD, 1);
		stream1.append(zset1_t1);

		const zset2_t1 = new ZSet();
		zset2_t1.add(itemA, 1); // A appears in both - should be deduplicated
		stream2.append(zset2_t1);

		// Execute: Perform union
		const unionResult = stream1.liftUnion(stream2);

		// Verify: Check results at each time point

		// t=0: Union of {A,B} and {B,C} should be {A,B,C}
		const result_t0 = unionResult.get(0);
		expect(result_t0.materialize).toHaveLength(3);
		expect(result_t0.materialize).toContainEqual(itemA);
		expect(result_t0.materialize).toContainEqual(itemB); // B only once due to distinct
		expect(result_t0.materialize).toContainEqual(itemC);

		// t=1: Union of {D} and {A} should be {A,D}
		const result_t1 = unionResult.get(1);
		expect(result_t1.materialize).toHaveLength(2);
		expect(result_t1.materialize).toContainEqual(itemA); // A only once due to distinct
		expect(result_t1.materialize).toContainEqual(itemD);

		// Verify stream properties
		expect(unionResult.length).toBe(2);
	});
	it('handles empty streams correctly', () => {
		const stream1 = new Stream();
		const stream2 = new Stream();

		const item = { id: 'X', name: 'Item X' };

		// stream1 is empty, stream2 has data
		const zset = new ZSet();
		zset.add(item, 1);
		stream2.append(zset);

		const unionResult = stream1.liftUnion(stream2);

		// Should equal stream2 (union with empty = identity)
		expect(unionResult.get(0).materialize).toEqual([item]);
	});

	it('handles streams of different lengths', () => {
		const stream1 = new Stream(); // length 1
		const stream2 = new Stream(); // length 3

		const itemA = { id: 'A', name: 'A' };
		const itemB = { id: 'B', name: 'B' };
		const itemC = { id: 'C', name: 'C' };

		// stream1: just one element
		const zset1 = new ZSet();
		zset1.add(itemA, 1);
		stream1.append(zset1);

		// stream2: three elements
		const zset2_t0 = new ZSet();
		zset2_t0.add(itemB, 1);
		stream2.append(zset2_t0);

		const zset2_t1 = new ZSet();
		zset2_t1.add(itemC, 1);
		stream2.append(zset2_t1);

		const zset2_t2 = new ZSet();
		zset2_t2.add(itemA, 1); // Same as stream1[0]
		stream2.append(zset2_t2);

		const unionResult = stream1.liftUnion(stream2);

		// t=0: {A} ∪ {B} = {A,B}
		expect(unionResult.get(0).materialize).toHaveLength(2);

		// t=1: {} ∪ {C} = {C} (stream1[1] is empty)
		expect(unionResult.get(1).materialize).toEqual([itemC]);

		// t=2: {} ∪ {A} = {A}
		expect(unionResult.get(2).materialize).toEqual([itemA]);

		expect(unionResult.length).toBe(3); // Max of both stream lengths
	});

	it('verifies DBSP mathematical property: union is distinct(I1 + I2)', () => {
		const stream1 = new Stream();
		const stream2 = new Stream();

		const item = { id: 'X', name: 'Test' };

		// Both streams have same item with different weights
		const zset1 = new ZSet();
		zset1.add(item, 2); // weight 2
		stream1.append(zset1);

		const zset2 = new ZSet();
		zset2.add(item, 3); // weight 3
		stream2.append(zset2);

		const unionResult = stream1.liftUnion(stream2);

		// Manual calculation: distinct({item:2} + {item:3}) = distinct({item:5}) = {item:1}
		// Union should normalize to weight 1 regardless of input weights
		expect(unionResult.get(0).materialize).toEqual([item]);
		expect(unionResult.get(0).debug().get(JSON.stringify(item))).toBe(1);
	});
});

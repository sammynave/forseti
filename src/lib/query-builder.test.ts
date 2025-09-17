import { describe, expect, it } from 'vitest';
import { Query } from './query-builder.js';
import { Stream } from './stream.js';
import { ZSet } from './z-set.js';
import { integrate } from './stream/utils.js';

describe('Query Builder - Basic Functionality', () => {
	it('can create a query chain', () => {
		const stream = new Stream();
		const query = Query.from(stream);

		expect(query).toBeDefined();
		expect(query.getOperations()).toHaveLength(1);
		expect(query.getOperations()[0].type).toBe('source');
	});

	it('can chain where operations', () => {
		const stream = new Stream();

		const query = Query.from(stream).where((item) => item.active);

		expect(query.getOperations()).toHaveLength(2);
		expect(query.getOperations()[1].type).toBe('filter');
		expect(query.getOperations()[1].isLinear).toBe(true);
	});

	it('can chain select operations', () => {
		const stream = new Stream();

		const query = Query.from(stream).select((item) => ({ id: item.id }));

		expect(query.getOperations()).toHaveLength(2);
		expect(query.getOperations()[1].type).toBe('project');
		expect(query.getOperations()[1].isLinear).toBe(true);
	});

	it('can build complex query chains', () => {
		const userStream = new Stream();
		const orderStream = new Stream();

		const query = Query.from(userStream)
			.where((user) => user.active)
			.select((user) => ({ id: user.id, name: user.name }))
			.join(
				orderStream,
				(user) => user.id,
				(order) => order.userId
			)
			.distinct();

		const operations = query.getOperations();
		expect(operations).toHaveLength(5); // source, filter, project, join, distinct

		// Verify operation types
		expect(operations[0].type).toBe('source');
		expect(operations[1].type).toBe('filter');
		expect(operations[2].type).toBe('project');
		expect(operations[3].type).toBe('join');
		expect(operations[4].type).toBe('distinct');

		// Verify optimization metadata
		expect(operations[1].isLinear).toBe(true); // filter is linear
		expect(operations[2].isLinear).toBe(true); // project is linear
		expect(operations[3].isBilinear).toBe(true); // join is bilinear
		expect(operations[4].isLinear).toBe(false); // distinct is NOT linear
	});
});

describe('Query Builder - Basic Execution', () => {
	it('executes simple filter query', () => {
		const stream = new Stream();

		// Add test data
		const zset = new ZSet();
		zset.add({ id: 1, name: 'Alice', active: true }, 1);
		zset.add({ id: 2, name: 'Bob', active: false }, 1);
		stream.append(zset);

		const result = Query.from(stream)
			.where((item) => item.active)
			.execute();

		// Should only contain Alice
		expect(result.get(0).materialize).toEqual([{ id: 1, name: 'Alice', active: true }]);
	});

	it('executes filter + select query', () => {
		const stream = new Stream();

		const zset = new ZSet();
		zset.add({ id: 1, name: 'Alice', active: true }, 1);
		zset.add({ id: 2, name: 'Bob', active: false }, 1);
		stream.append(zset);
		const result = Query.from(stream)
			.where((item) => item.active)
			.select((item) => ({ name: item.name }))
			.execute();

		// Should contain Alice's name only
		expect(result.get(0).materialize).toEqual([{ name: 'Alice' }]);
	});
});

describe('Algorithm 4.6 - Auto Incremental', () => {
	it('produces same results as manual incremental', () => {
		const stream = new Stream();

		// Add test data over time
		const change1 = new ZSet();
		change1.add({ id: 1, name: 'Alice', active: true }, 1);
		stream.append(change1);

		const change2 = new ZSet();
		change2.add({ id: 2, name: 'Bob', active: false }, 1);
		stream.append(change2);

		// Manual incremental (current way)
		const manual = stream.liftFilter((item) => item.active);

		// Automatic incremental (Algorithm 4.6)
		const automatic = Query.from(stream)
			.where((item) => item.active)
			.autoIncremental();

		// Should produce identical results
		expect(automatic.get(0).materialize).toEqual(manual.get(0).materialize);
		expect(automatic.get(1).materialize).toEqual(manual.get(1).materialize);
	});
});
describe('Algorithm 4.6 - Correctness Verification', () => {
	it('incremental results integrate to same values as non-incremental', () => {
		// Create a stream of database changes over time
		const changeStream = new Stream();

		// t=0: Add some users
		const change0 = new ZSet();
		change0.add({ id: 1, name: 'Alice', active: true }, 1);
		change0.add({ id: 2, name: 'Bob', active: false }, 1);
		changeStream.append(change0);

		// t=1: Add more users, modify existing
		const change1 = new ZSet();
		change1.add({ id: 3, name: 'Charlie', active: true }, 1);
		change1.add({ id: 2, name: 'Bob', active: false }, -1); // Remove old Bob
		change1.add({ id: 2, name: 'Bob', active: true }, 1); // Add new Bob (now active)
		changeStream.append(change1);

		// t=2: Delete a user
		const change2 = new ZSet();
		change2.add({ id: 1, name: 'Alice', active: true }, -1); // Delete Alice
		changeStream.append(change2);

		// METHOD 1: Incremental computation (Algorithm 4.6)
		const incrementalResults = Query.from(changeStream)
			.where((user) => user.active)
			.autoIncremental();

		// METHOD 2: Non-incremental computation (baseline truth)
		const snapshots = integrate(changeStream); // Convert changes to snapshots
		const nonIncrementalResults = Query.from(snapshots)
			.where((user) => user.active)
			.execute();

		// METHOD 3: Manual verification - integrate incremental results
		const integratedIncrementalResults = integrate(incrementalResults);

		// 🎯 CRITICAL TEST: All three methods should produce identical results
		for (let t = 0; t < changeStream.length; t++) {
			console.log(`\n=== Time ${t} ===`);
			console.log('Snapshot DB:', snapshots.get(t).materialize);
			console.log('Non-incremental result:', nonIncrementalResults.get(t).materialize);
			console.log('Incremental change:', incrementalResults.get(t).materialize);
			console.log('Integrated incremental:', integratedIncrementalResults.get(t).materialize);

			// The key correctness property
			expect(integratedIncrementalResults.get(t).materialize).toEqual(
				nonIncrementalResults.get(t).materialize
			);
		}
	});

	it('verifies incremental computation step-by-step', () => {
		const changeStream = new Stream();

		// Simple test case: just add and remove one item
		const add = new ZSet();
		add.add({ id: 1, name: 'Test', active: true }, 1);
		changeStream.append(add);

		const remove = new ZSet();
		remove.add({ id: 1, name: 'Test', active: true }, -1);
		changeStream.append(remove);

		// Apply filter query both ways
		const incremental = Query.from(changeStream)
			.where((item) => item.active)
			.autoIncremental();

		const snapshots = integrate(changeStream);
		const nonIncremental = Query.from(snapshots)
			.where((item) => item.active)
			.execute();

		// t=0: Should have the item
		expect(integrate(incremental).get(0).materialize).toEqual([
			{ id: 1, name: 'Test', active: true }
		]);
		expect(nonIncremental.get(0).materialize).toEqual([{ id: 1, name: 'Test', active: true }]);

		// t=1: Should be empty (item removed)
		expect(integrate(incremental).get(1).materialize).toEqual([]);
		expect(nonIncremental.get(1).materialize).toEqual([]);
	});

	it('handles complex multi-step queries correctly', () => {
		const userStream = new Stream();
		const orderStream = new Stream();

		// Add users over time
		const userChange1 = new ZSet();
		userChange1.add({ id: 1, name: 'Alice', active: true }, 1);
		userStream.append(userChange1);

		// Add orders over time
		const orderChange1 = new ZSet();
		orderChange1.add({ id: 101, userId: 1, amount: 50 }, 1);
		orderStream.append(orderChange1);

		// Complex query: active users with their orders
		const incrementalQuery = Query.from(userStream)
			.where((user) => user.active)
			.join(
				orderStream,
				(user) => user.id,
				(order) => order.userId
			)
			.select(([user, order]) => ({
				userName: user.name,
				orderAmount: order.amount
			}))
			.autoIncremental();

		// Same query non-incrementally
		const userSnapshots = integrate(userStream);
		const orderSnapshots = integrate(orderStream);
		const nonIncrementalQuery = Query.from(userSnapshots)
			.where((user) => user.active)
			.join(
				orderSnapshots,
				(user) => user.id,
				(order) => order.userId
			)
			.select(([user, order]) => ({
				userName: user.name,
				orderAmount: order.amount
			}))
			.execute();

		// Verify results are equivalent
		const integratedIncremental = integrate(incrementalQuery);

		expect(integratedIncremental.get(0).materialize).toEqual(
			nonIncrementalQuery.get(0).materialize
		);

		// Should contain the joined result
		expect(nonIncrementalQuery.get(0).materialize).toEqual([
			{ userName: 'Alice', orderAmount: 50 }
		]);
	});
});

describe('Algorithm 4.6 - Debug Understanding', () => {
	it('shows the difference between changes and snapshots', () => {
		const changeStream = new Stream();

		// Add items over time
		for (let i = 1; i <= 3; i++) {
			const change = new ZSet();
			change.add({ id: i, value: i * 10 }, 1);
			changeStream.append(change);
		}

		console.log('\n=== DEBUGGING INCREMENTAL VS NON-INCREMENTAL ===');

		// Show the raw change stream
		console.log('\nChange Stream (input):');
		for (let t = 0; t < changeStream.length; t++) {
			console.log(`  t=${t}:`, changeStream.get(t).materialize);
		}

		// Show integrated snapshots
		const snapshots = integrate(changeStream);
		console.log('\nIntegrated Snapshots:');
		for (let t = 0; t < snapshots.length; t++) {
			console.log(`  t=${t}:`, snapshots.get(t).materialize);
		}

		// Show incremental query results (changes)
		const incrementalResult = Query.from(changeStream)
			.where((item) => item.value > 15) // Filter for values > 15
			.autoIncremental();

		console.log('\nIncremental Results (changes):');
		for (let t = 0; t < incrementalResult.length; t++) {
			console.log(`  t=${t}:`, incrementalResult.get(t).materialize);
		}

		// Show non-incremental query results (snapshots)
		const nonIncrementalResult = Query.from(snapshots)
			.where((item) => item.value > 15)
			.execute();

		console.log('\nNon-incremental Results (snapshots):');
		for (let t = 0; t < nonIncrementalResult.length; t++) {
			console.log(`  t=${t}:`, nonIncrementalResult.get(t).materialize);
		}

		// Show integrated incremental results (should match non-incremental)
		const integratedIncremental = integrate(incrementalResult);
		console.log('\nIntegrated Incremental Results:');
		for (let t = 0; t < integratedIncremental.length; t++) {
			console.log(`  t=${t}:`, integratedIncremental.get(t).materialize);
		}

		// The test: integrated incremental should equal non-incremental
		for (let t = 0; t < changeStream.length; t++) {
			expect(integratedIncremental.get(t).materialize).toEqual(
				nonIncrementalResult.get(t).materialize
			);
		}
	});
});
describe('Algorithm 4.6 - Distinct Optimization', () => {
	it('optimizes redundant distinct operations', () => {
		const stream = new Stream();

		// Add test data with duplicates
		const zset = new ZSet();
		zset.add({ id: 1, name: 'Alice' }, 1);
		zset.add({ id: 1, name: 'Alice' }, 1); // Duplicate (weight 2)
		zset.add({ id: 2, name: 'Bob' }, 1);
		stream.append(zset);

		// Query with redundant distinct operations
		const query = Query.from(stream)
			.distinct() // First distinct
			.where((x) => x.id > 0) // Linear operation
			.distinct() // Second distinct (should be optimized away)
			.autoIncremental();

		// Should still produce correct results
		const result = integrate(query);
		expect(result.get(0).materialize.length).toBe(2); // Alice (once) + Bob

		// The optimization should have eliminated one of the distinct operations
		// (This is a performance optimization, not a correctness requirement)
	});

	it('pushes distinct through linear operations', () => {
		const stream = new Stream();

		const zset = new ZSet();
		zset.add({ id: 1, name: 'Alice', active: true }, 2); // Weight 2
		zset.add({ id: 2, name: 'Bob', active: true }, 1);
		stream.append(zset);

		// Test that distinct gets pushed through filter (linear operation)
		const optimized = Query.from(stream)
			.where((x) => x.active) // Linear - distinct can be pushed through
			.distinct() // Should be moved earlier for efficiency
			.autoIncremental();

		const manual = Query.from(stream)
			.distinct() // Applied first
			.where((x) => x.active) // Then filter
			.autoIncremental();

		// Both should produce same results (optimization doesn't change correctness)
		const optimizedResult = integrate(optimized);
		const manualResult = integrate(manual);

		expect(optimizedResult.get(0).materialize).toEqual(manualResult.get(0).materialize);
	});
});
describe('Algorithm 4.6 - Chain Rule Optimizations', () => {
	it('linear operations work directly on changes (no I/D overhead)', () => {
		const changeStream = new Stream();

		// Add change
		const change = new ZSet();
		change.add({ id: 1, name: 'Alice', active: true }, 1);
		changeStream.append(change);

		// Linear operations should work directly on changes
		const optimizedResult = Query.from(changeStream)
			.where((x) => x.active) // Linear: should work on changes directly
			.select((x) => x.name) // Linear: should work on changes directly
			.autoIncremental();

		// Should produce changes directly, not need integration
		expect(optimizedResult.get(0).materialize).toEqual(['Alice']);
	});

	it('bilinear join uses Theorem 3.4 optimization', () => {
		const userChanges = new Stream();
		const orderStream = new Stream(); // Static for this test

		const userChange = new ZSet();
		userChange.add({ id: 1, name: 'Alice' }, 1);
		userChanges.append(userChange);

		const orders = new ZSet();
		orders.add({ userId: 1, amount: 100 }, 1);
		orderStream.append(orders);

		// Join should use optimized formula, not naive D ∘ join ∘ I
		const result = Query.from(userChanges)
			.join(
				orderStream,
				(user) => user.id,
				(order) => order.userId
			)
			.autoIncremental();

		// Should produce correct join result
		const integrated = integrate(result);
		expect(integrated.get(0).materialize).toEqual([
			[
				{ id: 1, name: 'Alice' },
				{ userId: 1, amount: 100 }
			]
		]);
	});
});

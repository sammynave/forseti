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

describe('Query Union Integration', () => {
	it('executes union query in regular (non-incremental) mode', () => {
		// Create two streams with test data
		const stream1 = new Stream();
		const stream2 = new Stream();

		// Add different items to each stream
		const zset1 = new ZSet();
		zset1.add({ id: 'A', type: 'premium' }, 1);
		stream1.append(zset1);

		const zset2 = new ZSet();
		zset2.add({ id: 'B', type: 'basic' }, 1);
		stream2.append(zset2);

		// Execute union query
		const result = Query.from(stream1).union(Query.from(stream2)).execute();

		// Should contain both items
		expect(result.get(0).materialize).toHaveLength(2);
		expect(result.get(0).materialize).toContainEqual({ id: 'A', type: 'premium' });
		expect(result.get(0).materialize).toContainEqual({ id: 'B', type: 'basic' });
	});
	it('executes union query in incremental mode', () => {
		// This tests the entire refactored architecture
		const stream1 = new Stream();
		const stream2 = new Stream();

		// Setup initial data
		const zset1 = new ZSet();
		zset1.add({ id: 'A', active: true }, 1);
		stream1.append(zset1);

		const zset2 = new ZSet();
		zset2.add({ id: 'B', active: true }, 1);
		stream2.append(zset2);

		// Execute incremental union query
		const result = Query.from(stream1)
			.where((item) => item.active)
			.union(Query.from(stream2).where((item) => item.active))
			.autoIncremental(); // This tests the new Circuit-based approach!

		// Verify results
		expect(result.get(0).materialize).toHaveLength(2);
		expect(result.get(0).materialize).toStrictEqual([
			{ id: 'A', active: true },
			{ id: 'B', active: true }
		]);
	});
	it('optimizes union with distinct elimination rules', () => {
		// Test that Proposition 4.4/4.5 optimizations work with union
		const stream1 = new Stream();
		const stream2 = new Stream();

		// Add test data to both streams
		const zset1 = new ZSet();
		zset1.add({ id: 'A', type: 'premium' }, 1);
		zset1.add({ id: 'B', type: 'premium' }, 2); // Weight 2 to test distinct
		stream1.append(zset1);

		const zset2 = new ZSet();
		zset2.add({ id: 'C', type: 'basic' }, 1);
		zset2.add({ id: 'D', type: 'basic' }, 3); // Weight 3 to test distinct
		stream2.append(zset2);

		const result = Query.from(stream1)
			.where((item) => item.type === 'premium')
			.distinct()
			.union(Query.from(stream2).where((item) => item.type === 'basic'))
			.distinct()
			.autoIncremental();

		// Should work without errors AND produce correct results
		const integrated = integrate(result);
		const materialized = integrated.get(0).materialize;

		// Should contain all 4 items, each with weight 1 due to distinct
		expect(materialized).toHaveLength(4);
		expect(materialized).toContainEqual({ id: 'A', type: 'premium' });
		expect(materialized).toContainEqual({ id: 'B', type: 'premium' });
		expect(materialized).toContainEqual({ id: 'C', type: 'basic' });
		expect(materialized).toContainEqual({ id: 'D', type: 'basic' });

		// Verify distinct worked (all weights should be 1, not original weights)
		const debugResult = integrated.get(0).debug();
		for (const [key, weight] of debugResult) {
			expect(weight).toBe(1); // distinct should normalize all weights to 1
		}
	});
});
describe('StreamingProcessor - Incremental Operations', () => {
	it('initializes with correct state from snapshot', () => {
		// Create initial snapshot with test data
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', active: true }, 1);
		initialSnapshot.add({ id: 'B', active: false }, 1);
		initialSnapshot.add({ id: 'C', active: true }, 1);

		// Create processor with filter
		const processor = Query.from<any>(new Stream())
			.where((item) => item.active)
			.createStreamingProcessor(initialSnapshot);

		// Should start with filtered initial state
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(2);
		expect(currentState.materialize).toStrictEqual([
			{ id: 'A', active: true },
			{ id: 'C', active: true }
		]);
	});

	it('processes filter changes incrementally', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', active: true }, 1);

		const processor = Query.from<any>(new Stream())
			.where((item) => item.active)
			.createStreamingProcessor(initialSnapshot);

		// Process a change: add new active item
		const change = new ZSet();
		change.add({ id: 'B', active: true }, 1);
		change.add({ id: 'C', active: false }, 1); // Should be filtered out

		processor.processChange(change);

		// Should have A (initial) + B (new active)
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(2);
		expect(currentState.materialize).toStrictEqual([
			{ id: 'A', active: true },
			{ id: 'B', active: true }
		]);
	});

	it('processes project changes incrementally', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', name: 'Alice', active: true }, 1);

		const processor = Query.from<any>(new Stream())
			.select((item) => ({ name: item.name }))
			.createStreamingProcessor(initialSnapshot);

		// Initial state should be projected
		expect(processor.getCurrentState().materialize).toEqual([{ name: 'Alice' }]);

		// Process change
		const change = new ZSet();
		change.add({ id: 'B', name: 'Bob', active: false }, 1);

		processor.processChange(change);

		// Should have both projected names
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(2);
		expect(currentState.materialize).toStrictEqual([{ name: 'Alice' }, { name: 'Bob' }]);
	});

	it('processes distinct changes incrementally', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ type: 'A' }, 2); // Weight 2 - should become 1

		const processor = Query.from<any>(new Stream())
			.distinct()
			.createStreamingProcessor(initialSnapshot);

		// Initial state should be distinct (weight 1)
		expect(processor.getCurrentState().debug().get('{"type":"A"}')).toBe(1);

		// Process change: add same item with weight 3
		const change = new ZSet();
		change.add({ type: 'A' }, 3);

		processor.processChange(change);

		// Should still have weight 1 (distinct)
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toStrictEqual([{ type: 'A' }]);
		expect(currentState.debug().get('{"type":"A"}')).toBe(1);
	});

	it('processes join changes incrementally', () => {
		// Create initial data
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', userId: 1 }, 1);

		// Create other stream with user data
		const userStream = new Stream();
		const userZSet = new ZSet();
		userZSet.add({ userId: 1, name: 'Alice' }, 1);
		userStream.append(userZSet);

		const processor = Query.from<any>(new Stream())
			.join(
				userStream,
				(item) => item.userId,
				(user) => user.userId
			)
			.createStreamingProcessor(initialSnapshot);

		// Should have initial join result
		const initialState = processor.getCurrentState().materialize;
		expect(initialState).toHaveLength(1);
		expect(initialState[0]).toEqual([
			{ id: 'A', userId: 1 },
			{ userId: 1, name: 'Alice' }
		]);

		// Process change: add item that matches existing user
		const change = new ZSet();
		change.add({ id: 'B', userId: 1 }, 1);

		processor.processChange(change);

		// Should have both join results
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(2);
		expect(currentState.materialize).toStrictEqual([
			[
				{
					id: 'A',
					userId: 1
				},
				{
					name: 'Alice',
					userId: 1
				}
			],
			[
				{
					id: 'B',
					userId: 1
				},
				{
					name: 'Alice',
					userId: 1
				}
			]
		]);
	});

	it('processes union changes incrementally', () => {
		// Create two initial snapshots
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ type: 'A', id: 1 }, 1);

		// Create other query source
		const otherStream = new Stream();
		const otherZSet = new ZSet();
		otherZSet.add({ type: 'B', id: 2 }, 1);
		otherStream.append(otherZSet);

		const processor = Query.from<any>(new Stream())
			.union(Query.from(otherStream))
			.createStreamingProcessor(initialSnapshot);

		// Should have union of both initial states
		const initialState = processor.getCurrentState();
		expect(initialState.materialize).toHaveLength(2);
		expect(initialState.materialize).toContainEqual({ type: 'A', id: 1 });
		expect(initialState.materialize).toContainEqual({ type: 'B', id: 2 });

		// Process change
		const change = new ZSet();
		change.add({ type: 'C', id: 3 }, 1);

		processor.processChange(change);

		// Should have all three items
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(3);
	});

	it('handles complex query chains', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', type: 'premium', active: true }, 1);
		initialSnapshot.add({ id: 'B', type: 'basic', active: false }, 1);
		initialSnapshot.add({ id: 'C', type: 'premium', active: true }, 1);

		const processor = Query.from<any>(new Stream())
			.where((item) => item.active)
			.where((item) => item.type === 'premium')
			.select((item) => ({ id: item.id, tier: 'PREMIUM' }))
			.distinct()
			.createStreamingProcessor(initialSnapshot);

		// Should start with filtered and projected data
		const initialState = processor.getCurrentState();
		expect(initialState.materialize).toHaveLength(2);
		expect(initialState.materialize).toContainEqual({ id: 'A', tier: 'PREMIUM' });
		expect(initialState.materialize).toContainEqual({ id: 'C', tier: 'PREMIUM' });

		// Process change
		const change = new ZSet();
		change.add({ id: 'D', type: 'premium', active: true }, 1);
		change.add({ id: 'E', type: 'basic', active: true }, 1); // Should be filtered out

		processor.processChange(change);

		// Should have all premium active users
		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(3);
		expect(currentState.materialize).toContainEqual({ id: 'D', tier: 'PREMIUM' });
	});
});
describe('StreamingProcessor - Edge Cases & Error Handling', () => {
	it('handles join with empty otherStream', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', userId: 1 }, 1);

		// Create empty stream
		const emptyStream = new Stream();

		const processor = Query.from<any>(new Stream())
			.join(
				emptyStream,
				(item) => item.userId,
				(user) => user.userId
			)
			.createStreamingProcessor(initialSnapshot);

		// Should handle empty stream gracefully (no joins possible)
		const initialState = processor.getCurrentState();
		expect(initialState.materialize).toHaveLength(0);

		// Process change with empty join stream
		const change = new ZSet();
		change.add({ id: 'B', userId: 1 }, 1);

		const result = processor.processChange(change);

		// Should still handle gracefully
		expect(processor.getCurrentState().materialize).toHaveLength(0);
	});

	it('handles union with empty otherQuery stream', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ type: 'A', id: 1 }, 1);

		// Create empty stream for other query
		const emptyStream = new Stream();

		const processor = Query.from<any>(new Stream())
			.union(Query.from(emptyStream))
			.createStreamingProcessor(initialSnapshot);

		// Should only have items from main query (other stream is empty)
		const initialState = processor.getCurrentState();
		expect(initialState.materialize).toHaveLength(1);
		expect(initialState.materialize).toContainEqual({ type: 'A', id: 1 });

		// Process change - should still work with empty other stream
		const change = new ZSet();
		change.add({ type: 'B', id: 2 }, 1);

		processor.processChange(change);

		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(2);
		expect(currentState.materialize).toContainEqual({ type: 'A', id: 1 });
		expect(currentState.materialize).toContainEqual({ type: 'B', id: 2 });
	});

	it('hasDistinctOperation() helper works correctly', () => {
		// Query WITHOUT distinct
		const processorWithoutDistinct = Query.from<any>(new Stream())
			.where((item) => item.active)
			.select((item) => item.name)
			.createStreamingProcessor();

		// Query WITH distinct
		const processorWithDistinct = Query.from<any>(new Stream())
			.where((item) => item.active)
			.distinct()
			.createStreamingProcessor();

		// Test via the behavior difference (indirect test of hasDistinctOperation)
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ name: 'Alice', active: true }, 2);

		const withoutDistinct = Query.from<any>(new Stream())
			.select((item) => item.name)
			.createStreamingProcessor(initialSnapshot);

		const withDistinct = Query.from<any>(new Stream())
			.select((item) => item.name)
			.distinct()
			.createStreamingProcessor(initialSnapshot);

		// Add same change to both
		const change = new ZSet();
		change.add({ name: 'Alice', active: true }, 1);

		withoutDistinct.processChange(change);
		withDistinct.processChange(change);

		// Without distinct: weight should accumulate
		expect(withoutDistinct.getCurrentState().debug().get('"Alice"')).toBe(3); // 2 + 1

		// With distinct: weight should be normalized to 1
		expect(withDistinct.getCurrentState().debug().get('"Alice"')).toBe(1);
	});

	it('source operations are skipped in processChange', () => {
		// This is harder to test directly, but we can verify via spy or indirect behavior
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', active: true }, 1);

		const processor = Query.from<any>(new Stream())
			.where((item) => item.active) // This should be processed
			.createStreamingProcessor(initialSnapshot);

		// The source operation should exist but be skipped
		const operations = processor['operations']; // Access private for testing
		expect(operations).toHaveLength(2); // source + filter
		expect(operations[0].type).toBe('source');

		// Process a change - should work despite having source operation
		const change = new ZSet();
		change.add({ id: 'B', active: true }, 1);
		change.add({ id: 'C', active: false }, 1); // Should be filtered out

		processor.processChange(change);

		const currentState = processor.getCurrentState();
		expect(currentState.materialize).toHaveLength(2); // A + B (C filtered out)
	});

	it('handles multiple distinct operations in chain', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ type: 'A', value: 1 }, 3); // Weight 3

		const processor = Query.from<any>(new Stream())
			.distinct() // First distinct
			.where((item) => true) // Linear operation
			.distinct() // Second distinct (should still work)
			.createStreamingProcessor(initialSnapshot);

		// Initial state should be distinct
		expect(processor.getCurrentState().debug().get('{"type":"A","value":1}')).toBe(1);

		// Add same item with different weight
		const change = new ZSet();
		change.add({ type: 'A', value: 1 }, 5);

		processor.processChange(change);

		// Should still be weight 1 due to distinct
		expect(processor.getCurrentState().debug().get('{"type":"A","value":1}')).toBe(1);
	});

	it('handles complex union with distinct operations', () => {
		const initialSnapshot = new ZSet();
		initialSnapshot.add({ id: 'A', type: 'premium' }, 2); // Weight 2

		const otherStream = new Stream();
		const otherZSet = new ZSet();
		otherZSet.add({ id: 'B', type: 'basic' }, 3); // Weight 3
		otherStream.append(otherZSet);

		const processor = Query.from<any>(new Stream())
			.distinct()
			.union(Query.from(otherStream).distinct())
			.distinct() // Final distinct
			.createStreamingProcessor(initialSnapshot);

		// Should have both items with weight 1
		const initialState = processor.getCurrentState();
		expect(initialState.materialize).toHaveLength(2);
		expect(initialState.debug().get('{"id":"A","type":"premium"}')).toBe(1);
		expect(initialState.debug().get('{"id":"B","type":"basic"}')).toBe(1);
	});
});

describe('StreamingProcessor - Most Complex Query', () => {
	it('handles complex query', () => {
		// Simpler but still complex: Premium user order analytics with categories

		const usersStream = new Stream();
		const usersZSet = new ZSet();
		usersZSet.add({ userId: 1, name: 'Alice', tier: 'premium', totalSpent: 1200 }, 1);
		usersZSet.add({ userId: 2, name: 'Bob', tier: 'basic', totalSpent: 800 }, 1);
		usersStream.append(usersZSet);

		const ordersStream = new Stream();
		const ordersZSet = new ZSet();
		ordersZSet.add({ userId: 1, productId: 'electronics', amount: 400 }, 1);
		ordersZSet.add({ userId: 2, productId: 'books', amount: 300 }, 1);
		ordersStream.append(ordersZSet);

		// Complex query: Premium users with electronics orders + high spenders union
		const premiumElectronicsQuery = Query.from(usersStream)
			.where((user) => user.tier === 'premium')
			.join(
				ordersStream,
				(user) => user.userId,
				(order) => order.userId
			)
			.where(([user, order]) => order.productId === 'electronics')
			.select(([user, order]) => ({
				userId: user.userId,
				name: user.name,
				category: 'PremiumElectronics',
				amount: order.amount
			}))
			.distinct();

		const highSpenderQuery = Query.from(usersStream)
			.where((user) => user.totalSpent >= 1000)
			.select((user) => ({
				userId: user.userId,
				name: user.name,
				category: 'HighSpender',
				amount: user.totalSpent
			}));

		const masterQuery = premiumElectronicsQuery
			.union(highSpenderQuery)
			.distinct()
			.where((item) => item.amount > 300)
			.select((item) => ({
				id: `${item.category}_${item.userId}`,
				profile: item.name,
				segment: item.category,
				value: item.amount
			}));

		const processor = masterQuery.createStreamingProcessor(usersStream.get(0));
		const result = processor.getCurrentState();

		expect(result.materialize).toHaveLength(2); // Should have both Alice entries
		console.log('Complex query result:', result.materialize);
	});
	it('handles complex e-commerce analytics query', () => {
		// === Setup: E-commerce data streams ===

		// Users stream
		const usersStream = new Stream();
		const usersZSet = new ZSet();
		usersZSet.add({ userId: 1, name: 'Alice', tier: 'premium', totalSpent: 1200 }, 1);
		usersZSet.add({ userId: 2, name: 'Bob', tier: 'basic', totalSpent: 800 }, 1);
		usersZSet.add({ userId: 3, name: 'Charlie', tier: 'premium', totalSpent: 1500 }, 1);
		usersStream.append(usersZSet);

		// Orders stream
		const ordersStream = new Stream();
		const ordersZSet = new ZSet();
		ordersZSet.add({ orderId: 101, userId: 1, productId: 201, amount: 400 }, 1);
		ordersZSet.add({ orderId: 102, userId: 2, productId: 202, amount: 300 }, 1);
		ordersZSet.add({ orderId: 103, userId: 3, productId: 201, amount: 500 }, 1);
		ordersStream.append(ordersZSet);

		// Products stream
		const productsStream = new Stream();
		const productsZSet = new ZSet();
		productsZSet.add({ productId: 201, name: 'iPhone', categoryId: 1, price: 400 }, 1);
		productsZSet.add({ productId: 202, name: 'Book', categoryId: 2, price: 300 }, 1);
		productsStream.append(productsZSet);

		// Categories stream
		const categoriesStream = new Stream();
		const categoriesZSet = new ZSet();
		categoriesZSet.add({ categoryId: 1, name: 'Electronics' }, 1);
		categoriesZSet.add({ categoryId: 2, name: 'Books' }, 1);
		categoriesStream.append(categoriesZSet);

		// Reviews stream
		const reviewsStream = new Stream();
		const reviewsZSet = new ZSet();
		reviewsZSet.add({ userId: 1, productId: 201, rating: 5, comment: 'Great!' }, 1);
		reviewsZSet.add({ userId: 3, productId: 201, rating: 4, comment: 'Good' }, 1);
		reviewsStream.append(reviewsZSet);

		// === THE COMPLEX QUERY ===
		// "Find premium users who bought Electronics AND wrote 4+ star reviews,
		//  UNION with users who spent $1000+, then get distinct user analytics"

		const premiumElectronicsReviewersQuery = Query.from(usersStream)
			.where((user) => user.tier === 'premium') // Filter: premium users
			.join(
				ordersStream,
				(user) => user.userId,
				(order) => order.userId
			) // Join: user orders
			.select(([user, order]) => ({
				userId: user.userId,
				name: user.name,
				tier: user.tier,
				totalSpent: user.totalSpent,
				orderId: order.orderId,
				productId: order.productId,
				amount: order.amount
			}))
			.join(
				productsStream,
				(item) => item.productId,
				(product) => product.productId
			) // Join: order products
			.select(([item, product]) => ({
				...item, // Keep all user+order properties
				productName: product.name,
				categoryId: product.categoryId,
				price: product.price
			}))
			.join(
				categoriesStream,
				(item) => item.categoryId,
				(category) => category.categoryId
			) // Join: product categories
			.where(([item, category]) => category.name === 'Electronics') // Filter: Electronics only
			.select(([item, category]) => item) // Project back to item
			.join(
				reviewsStream,
				(item) => `${item.userId}_${item.productId}`,
				(review) => `${review.userId}_${review.productId}`
			) // Complex join condition
			.where(([item, review]) => review.rating >= 4) // Filter: 4+ star reviews
			.select(([item, review]) => ({
				// Final projection
				userId: item.userId,
				userName: item.name,
				tier: item.tier,
				totalSpent: item.totalSpent,
				category: 'Electronics',
				rating: review.rating
			}))
			.distinct(); // Remove duplicates

		const highSpendersQuery = Query.from(usersStream)
			.where((user) => user.totalSpent >= 1000) // Filter: high spenders
			.select((user) => ({
				// Project to same format
				userId: user.userId,
				userName: user.name,
				tier: user.tier,
				totalSpent: user.totalSpent,
				category: 'HighSpender',
				rating: null
			}))
			.distinct();

		// MASTER QUERY: Union both subqueries + final distinct + complex projection
		const masterQuery = premiumElectronicsReviewersQuery
			.union(highSpendersQuery) // Union the subqueries
			.distinct() // Remove any final duplicates
			.where((item) => item.totalSpent > 500) // Additional filter
			.select((item) => ({
				// Final analytics projection
				analyticsId: `${item.category}_${item.userId}`,
				profile: {
					name: item.userName,
					tier: item.tier,
					spending: item.totalSpent
				},
				segment: item.category === 'Electronics' ? 'tech-savvy-premium' : 'high-value',
				qualityScore: item.rating || 'N/A'
			}));

		// === EXECUTE AND TEST ===
		const initialSnapshot = usersStream.get(0);

		// ADD THIS DEBUGGING SECTION:
		console.log('\n🔍 DEBUGGING COMPLEX QUERY CHAIN:');

		// Test each step individually
		const step1 = Query.from(usersStream)
			.where((user) => user.tier === 'premium')
			.createStreamingProcessor(initialSnapshot);
		console.log('Step 1 (premium users):', step1.getCurrentState().materialize);

		const step2 = Query.from(usersStream)
			.where((user) => user.tier === 'premium')
			.join(
				ordersStream,
				(user) => user.userId,
				(order) => order.userId
			)
			.createStreamingProcessor(initialSnapshot);
		console.log('Step 2 (+ orders join):', step2.getCurrentState().materialize);

		const step3 = Query.from(usersStream)
			.where((user) => user.tier === 'premium')
			.join(
				ordersStream,
				(user) => user.userId,
				(order) => order.userId
			)
			.select(([user, order]) => ({
				userId: user.userId,
				name: user.name,
				tier: user.tier,
				totalSpent: user.totalSpent,
				orderId: order.orderId,
				productId: order.productId,
				amount: order.amount
			}))
			.createStreamingProcessor(initialSnapshot);
		console.log('Step 3 (+ flatten):', step3.getCurrentState().materialize);

		const step4 =
			step3.getCurrentState().materialize.length > 0
				? Query.from(usersStream)
						.where((user) => user.tier === 'premium')
						.join(
							ordersStream,
							(user) => user.userId,
							(order) => order.userId
						)
						.select(([user, order]) => ({
							userId: user.userId,
							name: user.name,
							tier: user.tier,
							totalSpent: user.totalSpent,
							orderId: order.orderId,
							productId: order.productId,
							amount: order.amount
						}))
						.join(
							productsStream,
							(item) => item.productId,
							(product) => product.productId
						)
						.createStreamingProcessor(initialSnapshot)
				: null;
		console.log(
			'Step 4 (+ products join):',
			step4?.getCurrentState().materialize || 'SKIPPED - Step 3 empty'
		);

		// Continue debugging if step 4 worked...
		console.log('🔍 END DEBUGGING\n');

		const processor = masterQuery.createStreamingProcessor(initialSnapshot);

		const result = processor.getCurrentState();

		// Should have complex analytics results
		expect(result.materialize).toContainEqual({
			analyticsId: 'Electronics_1',
			profile: { name: 'Alice', tier: 'premium', spending: 1200 },
			segment: 'tech-savvy-premium',
			qualityScore: 5
		});

		expect(result.materialize).toContainEqual({
			analyticsId: 'HighSpender_3',
			profile: { name: 'Charlie', tier: 'premium', spending: 1500 },
			segment: 'high-value',
			qualityScore: 'N/A'
		});

		// Process incremental change: new high-spending user
		const change = new ZSet();
		change.add({ userId: 4, name: 'Diana', tier: 'premium', totalSpent: 2000 }, 1);
		// This should trigger the entire complex query incrementally!

		processor.processChange(change);

		const updatedResult = processor.getCurrentState();
		expect(updatedResult.materialize).toContainEqual({
			analyticsId: 'HighSpender_4',
			profile: { name: 'Diana', tier: 'premium', spending: 2000 },
			segment: 'high-value',
			qualityScore: 'N/A'
		});

		console.log('🎯 ULTRA-COMPLEX QUERY EXECUTED SUCCESSFULLY!');
		console.log('Operations count:', masterQuery.getOperations().length);
		console.log('Result:', updatedResult.materialize);
	});
});

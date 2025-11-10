import { describe, it, expect } from 'vitest';
import { ZSet, ZSetGroup } from '../lib/z-set.js';
import { ZSetOperators } from '../lib/z-set-operators.js';
import { generateOrders, generateUsers } from '../benchmarks/helpers.js';
import { createStatefulJoinCircuit } from '$lib/stateful-circuit.js';
import { StatefulTopK } from '$lib/stateful-top-k.js';

describe('DBSP: Incremental vs Batch Join Processing', () => {
	const USERS_COUNT = 2;
	const INITIAL_ORDERS = 2;
	const NEW_ORDERS = 1;

	const users = generateUsers(USERS_COUNT);
	const userIds = users.map((u) => u.id);
	const initialOrders = generateOrders(INITIAL_ORDERS, userIds);
	const newOrders = generateOrders(NEW_ORDERS, userIds);

	it('incremental should produce same result as batch', () => {
		const usersZSet = new ZSet(users.map((u) => [u, 1]));

		// INCREMENTAL APPROACH: Build result incrementally

		// Step 1: Process initial orders (setup phase)
		const initialOrdersZSet = new ZSet(initialOrders.map((o) => [o, 1]));
		const initialResult = ZSetOperators.equiJoin(
			initialOrdersZSet,
			usersZSet,
			(order) => order.userId,
			(user) => user.id
		);

		// Step 2: Process only the delta (new orders)
		const newOrdersZSet = new ZSet(newOrders.map((o) => [o, 1]));
		const deltaResult = ZSetOperators.equiJoin(
			newOrdersZSet,
			usersZSet,
			(order) => order.userId,
			(user) => user.id
		);

		// Step 3: Merge results (incremental view maintenance)
		const incrementalResult = initialResult.concat(deltaResult).mergeRecords();

		// BATCH APPROACH: Process everything from scratch
		const allOrdersZSet = new ZSet([...initialOrders, ...newOrders].map((o) => [o, 1]));
		const batchResult = ZSetOperators.equiJoin(
			allOrdersZSet,
			usersZSet,
			(order) => order.userId,
			(user) => user.id
		);

		// NOW THEY SHOULD BE EQUAL
		expect(incrementalResult.data).toStrictEqual(batchResult.data);
	});
});

describe('DBSP: Incremental vs Batch Join+Sort Processing', () => {
	const USERS_COUNT = 100;
	const INITIAL_ORDERS = 10;
	const NEW_ORDERS = 4;
	const TOP_K = 3;

	const users = generateUsers(USERS_COUNT);
	const userIds = users.map((u) => u.id);
	const initialOrders = generateOrders(INITIAL_ORDERS, userIds);
	const newOrders = generateOrders(NEW_ORDERS, userIds);

	it('incremental should produce same result as batch (with sorting)', () => {
		const usersZSet = new ZSet(users.map((u) => [u, 1]));

		// INCREMENTAL APPROACH: Build result incrementally with stateful operators

		// Step 1: Setup stateful circuits
		const statefulJoinCircuit = createStatefulJoinCircuit(
			(order) => order.userId,
			(user) => user.id
		);

		const statefulTopK = new StatefulTopK(
			([orderA], [orderB]) => orderB.amount - orderA.amount, // Descending by amount
			TOP_K,
			0,
			new ZSetGroup()
		);

		// Step 2: Process initial data
		const initialOrdersZSet = new ZSet(initialOrders.map((o) => [o, 1]));
		statefulJoinCircuit.initialize(initialOrdersZSet, usersZSet);
		const emptyUsers = new ZSet([]);
		const initialJoinResult = statefulJoinCircuit.processIncrement(initialOrdersZSet, emptyUsers);
		statefulTopK.processIncrement(initialJoinResult);

		// Step 3: Process delta (new orders)
		const newOrdersZSet = new ZSet(newOrders.map((o) => [o, 1]));
		const deltaJoinResult = statefulJoinCircuit.processIncrement(newOrdersZSet, emptyUsers);
		statefulTopK.processIncrement(deltaJoinResult);

		// Step 4: Get final incremental result
		const incrementalResult = statefulTopK.getCurrentState().topK;

		// BATCH APPROACH: Process everything from scratch
		const allOrdersZSet = new ZSet([...initialOrders, ...newOrders].map((o) => [o, 1]));

		// Step 1: Join all data
		const batchJoinResult = ZSetOperators.equiJoin(
			allOrdersZSet,
			usersZSet,
			(order) => order.userId,
			(user) => user.id
		);

		// Step 2: Sort all data
		const batchResult = ZSetOperators.topK(
			batchJoinResult,
			([orderA], [orderB]) => orderB.amount - orderA.amount,
			TOP_K
		);

		// Normalize results for comparison (sort by amount descending, then by ID for deterministic comparison)
		const normalizeForComparison = (zset) => {
			return zset.data
				.filter(([_, weight]) => weight > 0)
				.map(([[order, user]]) => ({
					orderId: order.id,
					orderAmount: order.amount,
					userId: order.userId,
					userName: user.name
				}))
				.sort((a, b) => {
					if (b.orderAmount !== a.orderAmount) {
						return b.orderAmount - a.orderAmount; // Descending by amount
					}
					return a.orderId - b.orderId; // Tie-break by ID for determinism
				});
		};

		const normalizedIncremental = normalizeForComparison(incrementalResult);
		const normalizedBatch = normalizeForComparison(batchResult);

		// NOW THEY SHOULD BE EQUAL
		expect(normalizedIncremental).toStrictEqual(normalizedBatch);
		expect(normalizedIncremental.length).toBeLessThanOrEqual(TOP_K);
	});

	it('should handle edge cases consistently', () => {
		// Test empty data
		const emptyResult = ZSetOperators.topK(
			new ZSet([]),
			([orderA], [orderB]) => orderB.amount - orderA.amount,
			TOP_K
		);
		expect(emptyResult.isEmpty()).toBe(true);

		// Test with fewer items than TOP_K
		const smallOrdersZSet = new ZSet(initialOrders.slice(0, 2).map((o) => [o, 1]));
		const usersZSet = new ZSet(users.map((u) => [u, 1]));

		const smallJoinResult = ZSetOperators.equiJoin(
			smallOrdersZSet,
			usersZSet,
			(order) => order.userId,
			(user) => user.id
		);

		const smallResult = ZSetOperators.topK(
			smallJoinResult,
			([orderA], [orderB]) => orderB.amount - orderA.amount,
			TOP_K
		);

		expect(smallResult.data.filter(([_, w]) => w > 0).length).toBeLessThanOrEqual(2);
	});
});

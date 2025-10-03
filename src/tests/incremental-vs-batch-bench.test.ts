import { describe, it, expect } from 'vitest';
import { ZSet } from '../lib/z-set.js';
import { ZSetOperators } from '../lib/z-set-operators.js';
import { generateOrders, generateUsers } from '../benchmarks/helpers.js';

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

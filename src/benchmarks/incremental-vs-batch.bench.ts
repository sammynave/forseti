import { describe, bench } from 'vitest';
import { ZSet, ZSetGroup } from '../lib/z-set.js';
import { ZSetOperators } from '../lib/z-set-operators.js';
import { createStatefulJoinCircuit } from '../lib/stateful-circuit.js';
import { generateOrders, generateUsers, type Order, type User } from './helpers.js';
import { StatefulTopK } from '$lib/stateful-top-k.js';
import { Circuit } from '$lib/circuit.js';
import { createTupleStream, Stream } from '$lib/stream.js';
import { createReactiveTable } from '$lib/reactive-table.js';
import { createQuery } from '$lib/query-builder.js';

describe('DBSP: True Incremental vs Batch Processing', () => {
	// Smaller dataset to focus on algorithmic difference
	const USERS_COUNT = 100;
	const INITIAL_ORDERS = 1000;
	const NEW_ORDERS = 100;

	const users = generateUsers(USERS_COUNT);
	const userIds = users.map((u) => u.id);
	const initialOrders = generateOrders(INITIAL_ORDERS, userIds);
	const newOrders = generateOrders(NEW_ORDERS, userIds);

	// Create stateful join circuit (maintains state between executions)
	const statefulJoinCircuit = createStatefulJoinCircuit<Order, User, string>(
		(order) => order.userId,
		(user) => user.id
	);

	// Initialize circuit with base data ONCE (not timed)
	const initialUsersZSet = new ZSet(users.map((u) => [u, 1]));
	const initialOrdersZSet = new ZSet(initialOrders.map((o) => [o, 1]));
	statefulJoinCircuit.initialize(initialOrdersZSet, initialUsersZSet);
	const e = new ZSet<User>([]);
	statefulJoinCircuit.processIncrement(initialOrdersZSet, e);

	bench('Incremental Streaming', () => {
		// TRUE STREAMING: Process each order individually and materialize after each
		const emptyUsersZSet = new ZSet<User>([]);

		for (const order of newOrders) {
			// Process single order delta
			const singleOrderZSet = new ZSet([[order, 1]]);
			statefulJoinCircuit.processIncrement(singleOrderZSet, emptyUsersZSet);

			// Materialize view after each delta (O(1) - cached!)
			const view = statefulJoinCircuit.getMaterializedView();

			if (view.isEmpty()) {
				console.error('Expected non-empty incremental result');
				throw new Error('Expected non-empty incremental result');
			}
		}
	});

	let currentOrders = [...initialOrders];
	const usersZSet = new ZSet(users.map((u) => [u, 1]));
	bench('Batch Streaming Recomputation', () => {
		// STREAMING BATCH: Recompute from scratch after each new order

		for (const order of newOrders) {
			// Add new order to dataset
			currentOrders.push(order);

			// Full recomputation from scratch (O(|DB|) every time!)
			const ordersZSet = new ZSet(currentOrders.map((o) => [o, 1]));
			const result = ZSetOperators.equiJoin(
				ordersZSet,
				usersZSet,
				(order) => order.userId,
				(user) => user.id
			);

			if (result.isEmpty()) {
				console.error('Expected non-empty batch result');
				throw new Error('Expected non-empty batch result');
			}
		}
	});
});

describe.skip('bench 1 change', () => {
	const USERS_COUNT = 10_000;
	const INITIAL_ORDERS = 100_000;
	const NEW_ORDERS = 100;

	const users = generateUsers(USERS_COUNT);
	const userIds = users.map((u) => u.id);
	const initialOrders = generateOrders(INITIAL_ORDERS, userIds);
	const newOrders = generateOrders(NEW_ORDERS, userIds);
	// const users = [{ id: 'user0', name: 'User 0', age: 63 }];
	// const initialOrders = [{ id: 0, userId: 'user0', amount: 644 }];
	// const newOrders = [{ id: 1, userId: 'user0', amount: 568 }]; // Different ID

	// Create stateful join circuit (maintains state between executions)
	const statefulJoinCircuit = createStatefulJoinCircuit<Order, User, string>(
		(order) => order.userId,
		(user) => user.id
	);

	// Initialize circuit with base data ONCE (not timed)
	// console.profile('inc');
	const initialUsersZSet = new ZSet(users.map((u) => [u, 1]));
	const initialOrdersZSet = new ZSet(initialOrders.map((o) => [o, 1]));
	// TODO
	// TODO
	// TODO
	// i don't think this is materializing the view the same way that processIncrement does
	statefulJoinCircuit.initialize(initialOrdersZSet, initialUsersZSet);

	const oneNew = generateOrders(1, userIds);
	const oneNewZ = new ZSet(oneNew.map((o) => [o, 1]));
	const initEmptyUsersZSet = new ZSet<User>([]);

	statefulJoinCircuit.processIncrement(oneNewZ, initEmptyUsersZSet);

	// Process only the new orders (delta) - users delta is empty

	// Process the increment (this returns only the delta)
	const newOrdersZSet = new ZSet(newOrders.map((o) => [o, 1]));
	const emptyUsersZSet = new ZSet<User>([]);
	bench('inc', () => {
		const deltaResult = statefulJoinCircuit.processIncrement(newOrdersZSet, emptyUsersZSet);

		// ========== INCREMENTAL APPROACH: Get complete final state ==========
		// Use the new getMaterializedView method for efficiency
		const incrementalCompleteResult = statefulJoinCircuit.getMaterializedView();
	});
	// ========== BATCH APPROACH: Full recomputation from scratch ==========
	const allOrders = [...initialOrders, ...oneNew, ...newOrders];
	const ordersZSet = new ZSet(allOrders.map((o) => [o, 1]));
	const usersZSet = new ZSet(users.map((u) => [u, 1]));
	bench('batch', () => {
		const batchResult = ZSetOperators.equiJoin(
			ordersZSet,
			usersZSet,
			(order) => order.userId,
			(user) => user.id
		);
	});
});

describe.skip('DBSP: True Incremental vs Batch Processing with Sorting', () => {
	// Smaller dataset to focus on algorithmic difference
	const USERS_COUNT = 100;
	const INITIAL_ORDERS = 1000;
	const NEW_ORDERS = 100;
	const TOP_K = 50; // Sort and keep top 50 results by order amount

	const users = generateUsers(USERS_COUNT);
	const userIds = users.map((u) => u.id);
	const initialOrders = generateOrders(INITIAL_ORDERS, userIds);
	const newOrders = generateOrders(NEW_ORDERS, userIds);

	// Create stateful join circuit (maintains state between executions)
	const statefulJoinCircuit = createStatefulJoinCircuit<Order, User, string>(
		(order) => order.userId,
		(user) => user.id
	);

	// Create stateful TopK circuit for sorting joined results
	const statefulTopK = new StatefulTopK<[Order, User]>(
		([orderA], [orderB]) => orderB.amount - orderA.amount, // Descending by order amount
		TOP_K,
		0,
		new ZSetGroup<[Order, User]>()
	);

	// Initialize circuits with base data ONCE (not timed)
	const initialUsersZSet = new ZSet(users.map((u) => [u, 1]));
	const initialOrdersZSet = new ZSet(initialOrders.map((o) => [o, 1]));

	// Initialize join circuit
	statefulJoinCircuit.initialize(initialOrdersZSet, initialUsersZSet);
	const e = new ZSet<User>([]);
	const initialJoinResult = statefulJoinCircuit.processIncrement(initialOrdersZSet, e);

	// Initialize TopK circuit with initial join results
	statefulTopK.processIncrement(initialJoinResult);

	bench('Incremental Streaming with Sorting', () => {
		// TRUE STREAMING: Process each order individually, maintain join + sorted results
		const emptyUsersZSet = new ZSet<User>([]);

		for (const order of newOrders) {
			// 1. Process single order delta through join (O(log |Users|))
			const singleOrderZSet = new ZSet([[order, 1]]);
			const joinDelta = statefulJoinCircuit.processIncrement(singleOrderZSet, emptyUsersZSet);

			// 2. Process join results through TopK sort (O(log K))
			const sortDelta = statefulTopK.processIncrement(joinDelta);

			// 3. Get current sorted view (O(1) - maintained incrementally!)
			const sortedView = statefulTopK.getCurrentState().topK;

			if (sortedView.isEmpty() && initialOrders.length > 0) {
				console.error('Expected non-empty incremental result');
				throw new Error('Expected non-empty incremental result');
			}
		}
	});

	let currentOrders = [...initialOrders];
	const usersZSet = new ZSet(users.map((u) => [u, 1]));

	bench('Batch Streaming Recomputation with Sorting', () => {
		// STREAMING BATCH: Full join + sort from scratch after each new order

		for (const order of newOrders) {
			// Add new order to dataset
			currentOrders.push(order);

			// 1. Full join recomputation from scratch (O(|Orders| * |Users|))
			const ordersZSet = new ZSet(currentOrders.map((o) => [o, 1]));
			const joinResult = ZSetOperators.equiJoin(
				ordersZSet,
				usersZSet,
				(order) => order.userId,
				(user) => user.id
			);

			// 2. Full sort from scratch (O(|JoinResult| * log |JoinResult|))
			const sortedResult = ZSetOperators.topK(
				joinResult,
				([orderA]: [Order, User], [orderB]: [Order, User]) => orderB.amount - orderA.amount,
				TOP_K
			);

			if (sortedResult.isEmpty() && currentOrders.length > 0) {
				console.error('Expected non-empty batch result');
				throw new Error('Expected non-empty batch result');
			}
		}
	});

	// Bonus: Circuit composition approach
	bench('Circuit Composition (Join + OrderBy)', () => {
		// Demonstrate composing join and orderBy circuits
		const joinCircuit = Circuit.equiJoin<Order, User, string>(
			(order) => order.userId,
			(user) => user.id
		);

		const sortCircuit = Circuit.orderBy<[Order, User], number>(
			([order]) => order.amount,
			(a, b) => b - a, // Descending
			{ limit: TOP_K }
		);

		const compositeCircuit = joinCircuit.compose(sortCircuit);

		// Create tuple streams for join input
		const orderStream = new Stream<ZSet<Order>>(new ZSet([]));
		const userStream = new Stream<ZSet<User>>(new ZSet([]));
		const tupleStream = createTupleStream(
			orderStream,
			userStream,
			new ZSetGroup<Order>(),
			new ZSetGroup<User>()
		);

		// Set initial data
		orderStream.set(0, initialOrdersZSet);
		userStream.set(0, initialUsersZSet);

		// Process incremental updates
		newOrders.forEach((order, index) => {
			orderStream.set(index + 1, new ZSet([[order, 1]]));
			userStream.set(index + 1, new ZSet<User>([])); // Empty user deltas
		});

		// Execute composite circuit
		const results = compositeCircuit.execute(tupleStream);

		// Verify results
		for (const [time, result] of results.entries()) {
			if (result.isEmpty() && time > 0) {
				console.error('Expected non-empty composite result');
				throw new Error('Expected non-empty composite result');
			}
		}
	});
});

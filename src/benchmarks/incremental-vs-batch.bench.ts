// src/benchmarks/incremental-vs-batch.bench.ts
import { describe, bench, expect } from 'vitest';
import { Stream, createTupleStream } from '../lib/stream.js';
import { ZSet, ZSetGroup } from '../lib/z-set.js';
import { ZSetOperators } from '../lib/z-set-operators.js';
import { Circuit } from '../lib/circuit.js';
import { generateOrders, generateUsers, type Order, type User } from './helpers.js';
import { integrate } from '$lib/operators/integrate.js';

describe('Incremental vs Non-Incremental Benchmarks', () => {
	// ========== SCENARIO 1: Small Changes to Large Dataset ==========
	describe.only('Large dataset, small changes (ideal for incremental)', () => {
		const USERS = generateUsers(1000);
		const USER_IDS = USERS.map((u) => u.id);
		const INITIAL_ORDERS = generateOrders(10000, USER_IDS);
		const NEW_ORDERS = generateOrders(100, USER_IDS); // Only 1% new data

		// ========== INCREMENTAL COMPUTATION ==========
		const incrementalJoin = Circuit.equiJoin<Order, User, string>(
			(order) => order.userId,
			(user) => user.id
		);

		// Setup streams
		const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
		orderChanges.set(0, new ZSet(INITIAL_ORDERS.map((o) => [o, 1])));
		orderChanges.set(1, new ZSet(NEW_ORDERS.map((o) => [o, 1])));

		const userChanges = new Stream<ZSet<User>>(new ZSet([]));
		userChanges.set(0, new ZSet(USERS.map((u) => [u, 1])));
		userChanges.set(1, new ZSet([])); // No user changes

		const inputStream = createTupleStream(orderChanges, userChanges, new ZSet([]), new ZSet([]));

		// Execute incremental computation - this gives us CHANGES
		const incrementalChanges = incrementalJoin.execute(inputStream);

		// ========== INTEGRATE THE CHANGES TO GET FINAL STATE ==========
		const resultGroup = new ZSetGroup<[Order, User]>();
		const integrateOp = integrate(resultGroup);
		const integratedResults = integrateOp(incrementalChanges);

		bench('incremental', () => {
			// Get the final integrated state (cumulative result)
			const incrementalFinalState = integratedResults.at(1); // Time 1 is the final time
			incrementalFinalState.data;
		});

		bench('non-incremental', () => {
			// ========== NON-INCREMENTAL COMPUTATION ==========
			// Simulate non-incremental: recompute everything from scratch
			const allOrders = new ZSet([
				...INITIAL_ORDERS.map((o) => [o, 1] as [Order, number]),
				...NEW_ORDERS.map((o) => [o, 1] as [Order, number])
			]);
			const allUsers = new ZSet(USERS.map((u) => [u, 1] as [User, number]));

			// Full recomputation
			const nonIncrementalResult = ZSetOperators.equiJoin(
				allOrders,
				allUsers,
				(order) => order.userId,
				(user) => user.id
			);
			nonIncrementalResult.data;
		});
	});

	// ========== SCENARIO 2: Many Small Updates ==========
	describe('Many small updates over time', () => {
		const USERS = generateUsers(100);
		const USER_IDS = USERS.map((u) => u.id);
		const UPDATE_COUNT = 100;
		const ORDERS_PER_UPDATE = 10;

		bench('Incremental Join - Many small updates', () => {
			const incrementalJoin = Circuit.equiJoin<Order, User, string>(
				(order) => order.userId,
				(user) => user.id
			);

			const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
			const userChanges = new Stream<ZSet<User>>(new ZSet([]));

			// Initial data
			userChanges.set(0, new ZSet(USERS.map((u) => [u, 1])));
			orderChanges.set(0, new ZSet([]));

			// Many small updates
			for (let i = 1; i <= UPDATE_COUNT; i++) {
				const newOrders = generateOrders(ORDERS_PER_UPDATE, USER_IDS);
				orderChanges.set(i, new ZSet(newOrders.map((o) => [o, 1])));
				userChanges.set(i, new ZSet([]));
			}

			const inputStream = createTupleStream(orderChanges, userChanges, new ZSet([]), new ZSet([]));

			const results = incrementalJoin.execute(inputStream);

			// Process all updates
			for (let i = 0; i <= UPDATE_COUNT; i++) {
				results.at(i);
			}
		});

		bench('Non-Incremental Join - Recompute after each update', () => {
			const group = new ZSetGroup<Order>();
			let cumulativeOrders = new ZSet<Order>([]);
			const allUsers = new ZSet(USERS.map((u) => [u, 1] as [User, number]));

			// Simulate recomputing everything after each update
			for (let i = 1; i <= UPDATE_COUNT; i++) {
				const newOrders = generateOrders(ORDERS_PER_UPDATE, USER_IDS);
				const newOrdersZSet = new ZSet(newOrders.map((o) => [o, 1] as [Order, number]));

				// Add to cumulative (simulating database state)
				cumulativeOrders = group.add(cumulativeOrders, newOrdersZSet);

				// Recompute entire join
				const result = ZSetOperators.equiJoin(
					cumulativeOrders,
					allUsers,
					(order) => order.userId,
					(user) => user.id
				);

				// Force evaluation
				result.data.length;
			}
		});
	});

	// ========== SCENARIO 3: Scaling Test ==========
	describe('Scaling behavior', () => {
		const DATA_SIZES = [100, 500, 1000, 5000];

		DATA_SIZES.forEach((size) => {
			const users = generateUsers(size);
			const userIds = users.map((u) => u.id);
			const orders = generateOrders(size * 10, userIds);
			const newOrders = generateOrders(size / 10, userIds); // 10% new data

			bench(`Incremental Join - ${size} users, ${size * 10} orders`, () => {
				const incrementalJoin = Circuit.equiJoin<Order, User, string>(
					(order) => order.userId,
					(user) => user.id
				);

				const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
				orderChanges.set(0, new ZSet(orders.map((o) => [o, 1])));
				orderChanges.set(1, new ZSet(newOrders.map((o) => [o, 1])));

				const userChanges = new Stream<ZSet<User>>(new ZSet([]));
				userChanges.set(0, new ZSet(users.map((u) => [u, 1])));
				userChanges.set(1, new ZSet([]));

				const inputStream = createTupleStream(
					orderChanges,
					userChanges,
					new ZSet([]),
					new ZSet([])
				);

				const results = incrementalJoin.execute(inputStream);
				results.at(0);
				results.at(1);
			});

			bench(`Non-Incremental Join - ${size} users, ${size * 10} orders`, () => {
				const allOrders = new ZSet([
					...orders.map((o) => [o, 1] as [Order, number]),
					...newOrders.map((o) => [o, 1] as [Order, number])
				]);
				const allUsers = new ZSet(users.map((u) => [u, 1] as [User, number]));

				const result = ZSetOperators.equiJoin(
					allOrders,
					allUsers,
					(order) => order.userId,
					(user) => user.id
				);

				result.data.length;
			});
		});
	});

	// ========== SCENARIO 4: Memory Usage Test ==========
	describe('Memory efficiency', () => {
		bench('Incremental Join - Memory usage over time', () => {
			const incrementalJoin = Circuit.equiJoin<Order, User, string>(
				(order) => order.userId,
				(user) => user.id
			);

			const users = generateUsers(1000);
			const userIds = users.map((u) => u.id);

			const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
			const userChanges = new Stream<ZSet<User>>(new ZSet([]));

			userChanges.set(0, new ZSet(users.map((u) => [u, 1])));

			// Simulate long-running process with many updates
			for (let i = 0; i < 1000; i++) {
				const orders = generateOrders(10, userIds);
				orderChanges.set(i, new ZSet(orders.map((o) => [o, 1])));
				if (i > 0) userChanges.set(i, new ZSet([]));
			}

			const inputStream = createTupleStream(orderChanges, userChanges, new ZSet([]), new ZSet([]));

			const results = incrementalJoin.execute(inputStream);

			// Process all updates
			for (let i = 0; i < 1000; i++) {
				results.at(i);
			}
		});
	});
});

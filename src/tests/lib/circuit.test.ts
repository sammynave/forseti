// src/tests/lib/circuit-incremental.test.ts
import { describe, it, expect } from 'vitest';
import { Stream, createTupleStream } from '../../lib/stream.js';
import { ZSet, ZSetGroup } from '../../lib/z-set.js';
import { ZSetOperators } from '../../lib/z-set-operators.js';
import { Circuit } from '../../lib/circuit.js';
import { generateOrders, generateUsers, type Order, type User } from '../../benchmarks/helpers.js';
import { integrate } from '$lib/operators/integrate.js';

describe('Circuit Incremental Computation', () => {
	describe('equiJoin incremental behavior', () => {
		interface Order {
			id: number;
			userId: string;
			amount: number;
		}

		interface User {
			id: string;
			name: string;
			age: number;
		}

		it('should produce same results as non-incremental computation', () => {
			// Create incremental join circuit
			const incrementalJoin = Circuit.equiJoin<Order, User, string>(
				(order) => order.userId,
				(user) => user.id
			);

			// ========== TIME 0: Initial data ==========
			const orders0 = new ZSet<Order>([
				[{ id: 1, userId: 'alice', amount: 100 }, 1],
				[{ id: 2, userId: 'bob', amount: 200 }, 1]
			]);

			const users0 = new ZSet<User>([
				[{ id: 'alice', name: 'Alice', age: 25 }, 1],
				[{ id: 'bob', name: 'Bob', age: 30 }, 1]
			]);

			// ========== TIME 1: Add new order and user ==========
			const orders1 = new ZSet<Order>([[{ id: 3, userId: 'carol', amount: 150 }, 1]]);

			const users1 = new ZSet<User>([[{ id: 'carol', name: 'Carol', age: 28 }, 1]]);

			// ========== TIME 2: Delete an order ==========
			const orders2 = new ZSet<Order>([
				[{ id: 1, userId: 'alice', amount: 100 }, -1] // deletion
			]);

			const users2 = new ZSet<User>([]);

			// Create change streams (what incremental computation receives)
			const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
			orderChanges.set(0, orders0);
			orderChanges.set(1, orders1);
			orderChanges.set(2, orders2);

			const userChanges = new Stream<ZSet<User>>(new ZSet([]));
			userChanges.set(0, users0);
			userChanges.set(1, users1);
			userChanges.set(2, users2);

			const inputStream = createTupleStream(orderChanges, userChanges, new ZSet([]), new ZSet([]));

			// Execute incremental computation
			const incrementalResults = incrementalJoin.execute(inputStream);

			// ========== VERIFICATION: Compare with non-incremental ==========

			// Manually compute what the full state should be at each time
			const group = new ZSetGroup<Order>();
			const userGroup = new ZSetGroup<User>();

			// Time 0: Full state
			let fullOrders0 = orders0;
			let fullUsers0 = users0;
			let expectedResult0 = ZSetOperators.equiJoin(
				fullOrders0,
				fullUsers0,
				(order) => order.userId,
				(user) => user.id
			);

			// Time 1: Full state after adding
			let fullOrders1 = group.add(fullOrders0, orders1);
			let fullUsers1 = userGroup.add(fullUsers0, users1);
			let expectedResult1 = ZSetOperators.equiJoin(
				fullOrders1,
				fullUsers1,
				(order) => order.userId,
				(user) => user.id
			);

			// Time 2: Full state after deletion
			let fullOrders2 = group.add(fullOrders1, orders2);
			let fullUsers2 = userGroup.add(fullUsers1, users2);
			let expectedResult2 = ZSetOperators.equiJoin(
				fullOrders2,
				fullUsers2,
				(order) => order.userId,
				(user) => user.id
			);

			// Compute expected changes (what incremental should output)
			const resultGroup = new ZSetGroup<[Order, User]>();
			let expectedChange0 = expectedResult0; // First result is the full result
			let expectedChange1 = resultGroup.subtract(expectedResult1, expectedResult0);
			let expectedChange2 = resultGroup.subtract(expectedResult2, expectedResult1);

			// Verify incremental results match expected changes
			expect(incrementalResults.at(0).data.sort()).toEqual(expectedChange0.data.sort());
			expect(incrementalResults.at(1).data.sort()).toEqual(expectedChange1.data.sort());
			expect(incrementalResults.at(2).data.sort()).toEqual(expectedChange2.data.sort());
		});

		it('should demonstrate incremental efficiency', () => {
			// This test shows that incremental computation produces correct deltas

			const incrementalJoin = Circuit.equiJoin<Order, User, string>(
				(order) => order.userId,
				(user) => user.id
			);

			// Start with some data
			const initialOrders = new ZSet<Order>([
				[{ id: 1, userId: 'alice', amount: 100 }, 1],
				[{ id: 2, userId: 'bob', amount: 200 }, 1]
			]);

			const initialUsers = new ZSet<User>([
				[{ id: 'alice', name: 'Alice', age: 25 }, 1],
				[{ id: 'bob', name: 'Bob', age: 30 }, 1]
			]);

			// Add just one new order
			const newOrder = new ZSet<Order>([
				[{ id: 3, userId: 'alice', amount: 50 }, 1] // Another order for Alice
			]);

			const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
			orderChanges.set(0, initialOrders);
			orderChanges.set(1, newOrder);

			const userChanges = new Stream<ZSet<User>>(new ZSet([]));
			userChanges.set(0, initialUsers);
			userChanges.set(1, new ZSet([])); // No user changes

			const inputStream = createTupleStream(orderChanges, userChanges, new ZSet([]), new ZSet([]));

			const results = incrementalJoin.execute(inputStream);

			// Time 0: Should have 2 join results (alice-Alice, bob-Bob)
			expect(results.at(0).data).toHaveLength(2);

			// Time 1: Should have only 1 NEW join result (new alice order with Alice user)
			// This proves incrementality - we only get the NEW result, not all results
			const time1Result = results.at(1);
			expect(time1Result.data).toHaveLength(1);
			expect(time1Result.data[0][0][0].id).toBe(3); // New order id
			expect(time1Result.data[0][0][1].name).toBe('Alice'); // Joined with Alice
			expect(time1Result.data[0][1]).toBe(1); // Weight 1 (addition)
		});

		it('should handle deletions correctly', () => {
			const incrementalJoin = Circuit.equiJoin<Order, User, string>(
				(order) => order.userId,
				(user) => user.id
			);

			// Initial state
			const orders = new ZSet<Order>([[{ id: 1, userId: 'alice', amount: 100 }, 1]]);

			const users = new ZSet<User>([[{ id: 'alice', name: 'Alice', age: 25 }, 1]]);

			// Delete the order
			const orderDeletion = new ZSet<Order>([
				[{ id: 1, userId: 'alice', amount: 100 }, -1] // Negative weight = deletion
			]);

			const orderChanges = new Stream<ZSet<Order>>(new ZSet([]));
			orderChanges.set(0, orders);
			orderChanges.set(1, orderDeletion);

			const userChanges = new Stream<ZSet<User>>(new ZSet([]));
			userChanges.set(0, users);
			userChanges.set(1, new ZSet([]));

			const inputStream = createTupleStream(orderChanges, userChanges, new ZSet([]), new ZSet([]));

			const results = incrementalJoin.execute(inputStream);

			// Time 0: Should create the join result
			expect(results.at(0).data).toHaveLength(1);
			expect(results.at(0).data[0][1]).toBe(1); // Weight 1 (addition)

			// Time 1: Should delete the join result
			expect(results.at(1).data).toHaveLength(1);
			expect(results.at(1).data[0][1]).toBe(-1); // Weight -1 (deletion)

			// The join record should be the same, just with opposite weight
			expect(results.at(1).data[0][0]).toEqual(results.at(0).data[0][0]);
		});
	});

	describe('ensure benchmark examples are the same', () => {
		it('integrated incremental results should match non-incremental', () => {
			const USERS = generateUsers(2);
			const USER_IDS = USERS.map((u) => u.id);
			const INITIAL_ORDERS = generateOrders(2, USER_IDS);
			const NEW_ORDERS = generateOrders(1, USER_IDS);

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

			// Get the final integrated state (cumulative result)
			const incrementalFinalState = integratedResults.at(1); // Time 1 is the final time

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

			// ========== COMPARISON ==========
			// Now these should match!
			expect(incrementalFinalState.data).toStrictEqual(nonIncrementalResult.data);
		});
	});
});

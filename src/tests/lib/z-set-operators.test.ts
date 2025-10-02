import { describe, it, expect, beforeEach } from 'vitest';
import { ZSet } from '../../lib/z-set.js';
import { ZSetOperators } from '$lib/z-set-operators.js';

describe('ZSetOperators', () => {
	let R: ZSet<string>;
	let S: ZSet<string>;
	let T: ZSet<string>;

	beforeEach(() => {
		// Examples from DBSP paper section 4.1
		R = new ZSet([
			['joe', 1],
			['anne', -1]
		]);
		S = new ZSet([
			['joe', 2],
			['bob', 1]
		]);
		T = new ZSet([
			['anne', 1],
			['charlie', 3]
		]);
	});
	describe('basic construction', () => {
		it('should create empty Z-set', () => {
			const empty = new ZSet([]);
			expect(empty.data).toEqual([]);
		});

		it('should create Z-set with elements', () => {
			const zset = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			expect(zset.data).toEqual([
				['joe', 1],
				['anne', -1]
			]);
		});

		it('should append new elements', () => {
			const zset = new ZSet([
				['joe', 1],
				['anne', -1]
			]);
			zset.append(['charlie', 2]);
			expect(zset.data).toEqual([
				['joe', 1],
				['anne', -1],
				['charlie', 2]
			]);
		});
	});

	describe('AGGREGATES', () => {
		it.todo('count', () => {});
		it.todo('sum', () => {});
		it.todo('min', () => {});
		it.todo('max', () => {});
	});
	describe('ALGEBRA', () => {
		describe('select', () => {
			it('can filter', () => {
				const result = ZSetOperators.filter(R, (name) => name === 'joe');
				expect(result.data).toStrictEqual([['joe', 1]]);
			});
			it('can filter', () => {
				const result = ZSetOperators.filter(R, (name) => name === 'anne');
				expect(result.data).toStrictEqual([['anne', -1]]);
			});
		});
	});

	describe('RELATIONAL', () => {
		describe('distinct operation', () => {
			it('should convert positive weights to 1, remove non-positive', () => {
				const mixed = new ZSet([
					['a', 3],
					['b', -1],
					['b', 3],
					['c', 1],
					['d', 0]
				]);
				const result = ZSetOperators.distinct(mixed);
				const expected = new ZSet([
					['a', 1],
					['b', 1],
					['c', 1]
				]);
				expect(result.data).toStrictEqual(expected.data);
			});
			it('should convert positive weights to 1, remove non-positive', () => {
				const mixed = new ZSet([
					['a', 3],
					['b', 1],
					['b', -3],
					['c', 1],
					['d', 0]
				]);
				const result = ZSetOperators.distinct(mixed);
				const expected = new ZSet([
					['a', 1],
					['c', 1]
				]);
				expect(result.data).toStrictEqual(expected.data);
			});

			it('should handle DBSP paper example', () => {
				// distinct(R) = {joe ↦ 1} (anne has negative weight)
				const result = ZSetOperators.distinct(R);
				const expected = new ZSet([['joe', 1]]);
				expect(result.data).toEqual(expected.data);
			});
		});
		describe.todo('join', () => {});
		describe.todo('union', () => {});
		describe.todo('except', () => {});
		describe.todo('intersect', () => {});
		describe.todo('where', () => {});
	});
	// ========== CARTESIAN PRODUCT TESTS ==========
	// Paper: (a × b)((x,y)) = a[x] × b[y]

	describe('cartesianProduct', () => {
		it('should compute cartesian product with weight multiplication', () => {
			const a = new ZSet([
				['x', 2],
				['y', 3]
			]);
			const b = new ZSet([
				['a', 1],
				['b', 4]
			]);

			const result = ZSetOperators.cartesianProduct(a, b);

			expect(result.data).toEqual([
				[['x', 'a'], 2], // 2 × 1 = 2
				[['x', 'b'], 8], // 2 × 4 = 8
				[['y', 'a'], 3], // 3 × 1 = 3
				[['y', 'b'], 12] // 3 × 4 = 12
			]);
		});

		it('should handle zero weights correctly', () => {
			const a = new ZSet([
				['x', 0],
				['y', 2]
			]);
			const b = new ZSet([['a', 3]]);

			const result = ZSetOperators.cartesianProduct(a, b);

			// Zero weights should be filtered out
			expect(result.data).toEqual([
				[['y', 'a'], 6] // Only non-zero result: 2 × 3 = 6
			]);
		});
	});

	// ========== EQUI-JOIN TESTS ==========
	// Paper: (a ⊲⊳ b)((x,y)) = a[x] × b[y] if x|c1 = y|c2

	describe('equiJoin', () => {
		it('should join records with matching keys', () => {
			const orders = new ZSet([
				[{ id: 1, userId: 'alice' }, 2],
				[{ id: 2, userId: 'bob' }, 1]
			]);
			const users = new ZSet([
				[{ id: 'alice', name: 'Alice' }, 3],
				[{ id: 'bob', name: 'Bob' }, 2]
			]);

			const result = ZSetOperators.equiJoin(
				orders,
				users,
				(order) => order.userId,
				(user) => user.id
			);

			expect(result.data).toEqual([
				[
					[
						{ id: 1, userId: 'alice' },
						{ id: 'alice', name: 'Alice' }
					],
					6
				], // 2 × 3 = 6
				[
					[
						{ id: 2, userId: 'bob' },
						{ id: 'bob', name: 'Bob' }
					],
					2
				] // 1 × 2 = 2
			]);
		});

		it('should not join records with non-matching keys', () => {
			const a = new ZSet([['x', 1]]);
			const b = new ZSet([['y', 2]]);

			const result = ZSetOperators.equiJoin(
				a,
				b,
				(x) => x,
				(y) => y
			);

			expect(result.data).toEqual([]); // No matches
		});
	});

	// ========== INTERSECTION TESTS ==========
	// Paper: Special case of equi-join when both relations have same schema

	describe('intersect', () => {
		it('should compute intersection with weight multiplication', () => {
			const a = new ZSet([
				['x', 2],
				['y', 3],
				['z', 1]
			]);
			const b = new ZSet([
				['x', 4],
				['y', 2]
			]);

			const result = ZSetOperators.intersect(a, b);

			expect(result.data).toEqual([
				['x', 8], // 2 × 4 = 8
				['y', 6] // 3 × 2 = 6
				// 'z' not in b, so not included
			]);
		});
	});

	// ========== FILTER TESTS ==========
	// Paper: σ_P(m)[x] = m[x] if P(x), 0 otherwise

	describe('filter', () => {
		it('should filter records based on predicate', () => {
			const zset = new ZSet([
				[{ age: 25, name: 'Alice' }, 2],
				[{ age: 17, name: 'Bob' }, 3],
				[{ age: 30, name: 'Carol' }, 1]
			]);

			const result = ZSetOperators.filter(zset, (person) => person.age >= 18);

			expect(result.data).toEqual([
				[{ age: 25, name: 'Alice' }, 2],
				[{ age: 30, name: 'Carol' }, 1]
				// Bob filtered out (age < 18)
			]);
		});

		it('should preserve weights for filtered records', () => {
			const zset = new ZSet([
				['x', -2],
				['y', 3]
			]);

			const result = ZSetOperators.filter(zset, (x) => x === 'x');

			expect(result.data).toEqual([['x', -2]]); // Negative weight preserved
		});
	});

	// ========== PROJECTION TESTS ==========
	// Paper: π(i)[y] = Σ_{x∈i, x|c=y} i[x]

	describe('project', () => {
		it('should project and merge weights for same projected values', () => {
			const zset = new ZSet([
				[{ name: 'Alice', dept: 'Engineering' }, 2],
				[{ name: 'Bob', dept: 'Engineering' }, 3],
				[{ name: 'Carol', dept: 'Sales' }, 1]
			]);

			const result = ZSetOperators.project(zset, (person) => person.dept);

			expect(result.data).toEqual([
				['Engineering', 5], // 2 + 3 = 5 (merged)
				['Sales', 1]
			]);
		});
	});

	// ========== DISTINCT TESTS ==========
	// Paper: distinct(m)[x] = 1 if m[x] > 0, 0 otherwise

	describe('distinct', () => {
		it('should convert positive weights to 1', () => {
			const zset = new ZSet([
				['x', 5],
				['y', 2],
				['z', 1]
			]);

			const result = ZSetOperators.distinct(zset);

			expect(result.data).toEqual([
				['x', 1],
				['y', 1],
				['z', 1]
			]);
		});

		it('should eliminate negative and zero weights', () => {
			const zset = new ZSet([
				['x', 3],
				['y', -2],
				['z', 0]
			]);

			const result = ZSetOperators.distinct(zset);

			expect(result.data).toEqual([
				['x', 1] // Only positive weight survives
			]);
		});

		it('should merge duplicate records before applying distinct', () => {
			const zset = new ZSet([
				['x', 2],
				['x', 3],
				['y', -1],
				['y', 2]
			]);

			const result = ZSetOperators.distinct(zset);

			expect(result.data).toEqual([
				['x', 1], // 2 + 3 = 5 > 0, becomes 1
				['y', 1] // -1 + 2 = 1 > 0, becomes 1
			]);
		});
	});

	// ========== SET OPERATIONS TESTS ==========

	describe('union', () => {
		it('should compute set union using distinct(a + b)', () => {
			const a = new ZSet([
				['x', 1],
				['y', 1]
			]);
			const b = new ZSet([
				['y', 1],
				['z', 1]
			]);

			const result = ZSetOperators.union(a, b);

			expect(result.data).toEqual([
				['x', 1],
				['y', 1], // Merged and distinct applied
				['z', 1]
			]);
		});
	});

	describe('difference', () => {
		it('should compute set difference using distinct(a - b)', () => {
			const a = new ZSet([
				['x', 1],
				['y', 1],
				['z', 1]
			]);
			const b = new ZSet([['y', 1]]);

			const result = ZSetOperators.difference(a, b);

			expect(result.data).toEqual([
				['x', 1],
				['z', 1]
				// 'y' eliminated: 1 - 1 = 0, filtered by distinct
			]);
		});
	});

	// ========== BILINEAR PROPERTY TESTS ==========
	// Verify key mathematical properties from the paper

	describe('bilinear properties', () => {
		it('cartesian product should be bilinear in first argument', () => {
			const a1 = new ZSet([['x', 2]]);
			const a2 = new ZSet([['y', 3]]);
			const b = new ZSet([['a', 1]]);

			// (a1 + a2) × b should equal (a1 × b) + (a2 × b)
			const left = ZSetOperators.cartesianProduct(a1.concat(a2), b);
			const right = ZSetOperators.cartesianProduct(a1, b).concat(
				ZSetOperators.cartesianProduct(a2, b)
			);

			expect(left.mergeRecords().data.sort()).toEqual(right.mergeRecords().data.sort());
		});
	});
});

import { describe, it, expect, beforeEach } from 'vitest';
import { ZSet, ZSetGroup } from '../../lib/z-set.js';

describe('ZSet', () => {
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
		const g = new ZSetGroup();
		describe('abelian group properties', () => {
			describe('addition (pointwise)', () => {
				it('should add Z-sets pointwise', () => {
					// R + S should combine weights for same elements
					const result = g.add(R, S);
					// Expected: {joe: 3, anne: -1, bob: 1}
					const expected = new ZSet([
						['joe', 3],
						['anne', -1],
						['bob', 1]
					]);
					expect(result.data.sort()).toEqual(expected.data.sort());
				});

				it('should handle disjoint Z-sets', () => {
					const A = new ZSet([
						['x', 2],
						['y', 1]
					]);
					const B = new ZSet([
						['z', 3],
						['w', -1]
					]);
					const result = g.add(A, B);
					const expected = new ZSet([
						['x', 2],
						['y', 1],
						['z', 3],
						['w', -1]
					]);
					expect(result.data.sort()).toEqual(expected.data.sort());
				});

				it('should satisfy commutativity: a + b = b + a', () => {
					const ab = g.add(R, S);
					const ba = g.add(S, R);
					expect(ab.data.sort()).toEqual(ba.data.sort());
				});

				it('should satisfy associativity: (a + b) + c = a + (b + c)', () => {
					const abc1a = g.add(R, S);
					const abc1 = g.add(abc1a, T);
					const abc2a = g.add(R, S);
					const abc2 = g.add(abc2a, T);
					expect(abc1.data.sort()).toEqual(abc2.data.sort());
				});

				it('should have identity element (empty Z-set)', () => {
					const empty = new ZSet([]);
					const result = g.add(R, empty);
					expect(result.data.sort()).toEqual(R.data.sort());
				});
			});

			describe('subtraction (pointwise)', () => {
				it('should subtract Z-sets pointwise', () => {
					// R - S: {joe: 1-2, anne: -1-0, bob: 0-1}
					const result = g.subtract(R, S);
					const expected = new ZSet([
						['joe', -1],
						['anne', -1],
						['bob', -1]
					]);
					expect(result.data.sort()).toEqual(expected.data.sort());
				});

				it('should handle elements only in first set', () => {
					const A = new ZSet([
						['x', 5],
						['y', 2]
					]);
					const B = new ZSet([['x', 2]]);
					const result = g.subtract(A, B);
					const expected = new ZSet([
						['x', 3],
						['y', 2]
					]);
					expect(result.data.sort()).toEqual(expected.data.sort());
				});

				it('should handle elements only in second set (negative weights)', () => {
					const A = new ZSet([['x', 3]]);
					const B = new ZSet([
						['x', 1],
						['y', 4]
					]);
					const result = g.subtract(A, B);
					const expected = new ZSet([
						['x', 2],
						['y', -4]
					]);
					expect(result.data).toStrictEqual(expected.data);
				});

				it('should satisfy a - b = a + (-b)', () => {
					const direct = g.subtract(R, S);
					const indirect = g.add(R, g.negate(S));
					expect(direct.data).toEqual(indirect.data);
				});

				it('should satisfy a - b = a + (-b)', () => {
					const a = new ZSet([
						['x', 2],
						['y', 1]
					]);
					const b = new ZSet([
						['x', 2],
						['y', -4]
					]);
					const result = g.subtract(a, b);
					expect(result.data).toStrictEqual([['y', 5]]);
				});
			});

			describe('negate', () => {
				it('should negate all weights', () => {
					const result = g.negate(R);
					const expected = new ZSet([
						['joe', -1],
						['anne', 1]
					]);
					expect(result.data.sort()).toEqual(expected.data.sort());
				});

				it('should satisfy a + (-a) = 0', () => {
					const result = g.add(R, g.negate(R));
					// Should result in empty Z-set (all weights cancel to 0)
					expect(result.data).toEqual([]);
				});
			});
		});
		describe('scalar multiplication', () => {
			it('should multiply all weights by scalar', () => {
				const result = R.multiply(2);
				const expected = new ZSet([
					['joe', 2],
					['anne', -2]
				]);
				expect(result.data.sort()).toEqual(expected.data.sort());
			});

			it('should handle zero multiplication', () => {
				const result = R.multiply(0);
				expect(result.data).toEqual([]);
			});
		});
		describe('filter', () => {
			it('can filter', () => {
				const result = R.filter((name) => name === 'joe');
				expect(result.data).toStrictEqual([['joe', 1]]);
			});
			it('can filter', () => {
				const result = R.filter((name) => name === 'anne');
				expect(result.data).toStrictEqual([['anne', -1]]);
			});
		});

		describe('mergeRecords', () => {
			it('should handle a zero case', () => {
				const zset = new ZSet([
					['joe', 1],
					['anne', -1],
					['joe', -1],
					['anne', 1]
				]);
				expect(zset.mergeRecords()).toStrictEqual(g.zero());
			});
			it('should handle a simple case', () => {
				const zset = new ZSet([
					['joe', 1],
					['anne', 2],
					['joe', 1],
					['anne', 1]
				]);
				expect(zset.mergeRecords().data).toStrictEqual([
					['joe', 2],
					['anne', 3]
				]);
			});
			it('should handle a non-primitive key', () => {
				const zset = new ZSet([
					[['joe', 'hi'], 1],
					[['anne', 'hey'], 2],
					[['joe', 'hi'], 1],
					[['anne', 'hey'], 1]
				]);
				expect(zset.mergeRecords().data).toStrictEqual([
					[['joe', 'hi'], 2],
					[['anne', 'hey'], 3]
				]);
			});

			it('should consolidate duplicate elements', () => {
				const duplicates = new ZSet([
					['a', 1],
					['a', 2],
					['b', 3],
					['b', -1]
				]);
				const result = duplicates.mergeRecords();
				const expected = new ZSet([
					['a', 3],
					['b', 2]
				]);
				expect(result.data.sort()).toEqual(expected.data.sort());
			});

			it('should remove zero weights', () => {
				const withZeros = new ZSet([
					['a', 1],
					['b', 0],
					['c', -2]
				]);
				const result = withZeros.mergeRecords();
				const expected = new ZSet([
					['a', 1],
					['c', -2]
				]);
				expect(result.data.sort()).toEqual(expected.data.sort());
			});
		});

		describe('concat', () => {
			it('should handle a simple case', () => {
				const zset = new ZSet([
					['joe', 1],
					['anne', -1],
					['joe', -1],
					['anne', 1]
				]);
				const zset2 = new ZSet([
					['joe', 1],
					['anne', -1]
				]);
				expect(zset.concat(zset2).data).toStrictEqual([
					['joe', 1],
					['anne', -1],
					['joe', -1],
					['anne', 1],
					['joe', 1],
					['anne', -1]
				]);
			});
		});
		describe('edge cases', () => {
			it('should handle empty Z-set operations', () => {
				const empty = new ZSet([]);
				const nonEmpty = new ZSet([['a', 1]]);

				expect(g.add(empty, nonEmpty).data).toEqual(nonEmpty.data);
				expect(g.subtract(empty, nonEmpty).data).toEqual([['a', -1]]);
				expect(g.negate(empty).data).toEqual([]);
			});

			it('should remove elements with zero weight after operations', () => {
				const a = new ZSet([
					['x', 2],
					['y', 1]
				]);
				const b = new ZSet([
					['x', 2],
					['z', 1]
				]);

				const result = g.subtract(a, b);
				// x: 2-2=0 should be removed, y: 1-0=1, z: 0-1=-1
				const expected = new ZSet([
					['y', 1],
					['z', -1]
				]);
				expect(result.data.sort()).toEqual(expected.data.sort());
			});
		});
		describe('complex element types', () => {
			it('should handle array elements', () => {
				const zset = new ZSet([
					[['apple', '$5'], 2],
					[['banana', '$2'], 1]
				]);
				const other = new ZSet([
					[['apple', '$5'], 1],
					[['kiwi', '$3'], 1]
				]);

				const result = g.add(zset, other);
				// Should combine weights for ['apple', '$5']
				expect(result.data).toContainEqual([['apple', '$5'], 3]);
			});

			it('should handle object elements', () => {
				const zset = new ZSet([
					[{ name: 'joe', age: 30 }, 1],
					[{ name: 'anne', age: 25 }, -1]
				]);
				const result = g.negate(zset);

				expect(result.data).toEqual([
					[{ name: 'joe', age: 30 }, -1],
					[{ name: 'anne', age: 25 }, 1]
				]);
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
				const result = mixed.distinct();
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
				const result = mixed.distinct();
				const expected = new ZSet([
					['a', 1],
					['c', 1]
				]);
				expect(result.data).toStrictEqual(expected.data);
			});

			it('should handle DBSP paper example', () => {
				// distinct(R) = {joe ↦ 1} (anne has negative weight)
				const result = R.distinct();
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

	describe('TYPE PREDICATES', () => {
		it('should check if Z-set represents a set (all weights = 1)', () => {
			const set = new ZSet([
				['a', 1],
				['b', 1]
			]);
			const notSet = new ZSet([
				['a', 2],
				['b', 1]
			]);

			expect(set.isSet()).toBe(true);
			expect(notSet.isSet()).toBe(false);
		});

		it('should check if Z-set is positive (all weights ≥ 0)', () => {
			const positive = new ZSet([
				['a', 1],
				['b', 2]
			]);
			const notPositive = new ZSet([
				['a', 1],
				['b', -1]
			]);

			expect(positive.isPositive()).toBe(true);
			expect(notPositive.isPositive()).toBe(false);
		});

		it('should check if Z-set is empty', () => {
			const empty = new ZSet([]);
			const notEmpty = new ZSet([['a', 1]]);

			expect(empty.isEmpty()).toBe(true);
			expect(notEmpty.isEmpty()).toBe(false);
		});
	});
});

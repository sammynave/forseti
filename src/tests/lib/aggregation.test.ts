import { describe, it, expect } from 'vitest';
import { ZSet } from '$lib/z-set.js';
import { ZSetOperators } from '$lib/z-set-operators.js';

describe('ZSetOperators Aggregation', () => {
	const data = new ZSet([
		[{ price: 10, qty: 2 }, 1],
		[{ price: 20, qty: 1 }, 1]
	]);

	it('count', () => {
		expect(ZSetOperators.count(data)).toBe(2);
	});

	it('sum', () => {
		expect(ZSetOperators.sum(data, (r) => r.price)).toBe(30);
	});

	it('average', () => {
		expect(ZSetOperators.average(data, (r) => r.price)).toBe(15);
	});

	it('groupBy', () => {
		const result = ZSetOperators.groupBy(data, (r) => r.qty);
		expect(result.size).toBe(2);
		expect(result.get(1)?.data.length).toBe(1);
		expect(result.get(2)?.data.length).toBe(1);
	});
});

describe('Aggregation Edge Cases', () => {
	it('empty data', () => {
		const empty = new ZSet([]);
		expect(ZSetOperators.count(empty)).toBe(0);
		expect(ZSetOperators.sum(empty, (r) => r.value)).toBe(0);
		expect(ZSetOperators.average(empty, (r) => r.value)).toBeNull();
	});

	it('negative weights', () => {
		const mixed = new ZSet([
			[{ value: 10 }, 2],
			[{ value: 5 }, -1]
		]);
		expect(ZSetOperators.count(mixed)).toBe(1); // 2 + (-1)
		expect(ZSetOperators.sum(mixed, (r) => r.value)).toBe(15); // 10*2 + 5*(-1)
	});
});

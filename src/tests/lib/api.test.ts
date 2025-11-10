import { AggregateBuilder, createQuery } from '$lib/query-builder.js';
import { createReactiveTable } from '$lib/reactive-table.js';
import { ZSet } from '$lib/z-set.js';
import { describe, it, expect } from 'vitest';
describe('QueryBuilder Aggregation', () => {
	it('reactive count', () => {
		const table = createReactiveTable(
			[
				{ id: 1, amount: 100 },
				{ id: 2, amount: 200 }
			],
			'id'
		);

		const count = createQuery(table).count();
		let result: number[] = [];
		const u = count.subscribe((value) => (result = value));

		expect(result).toEqual([2]);

		table.add({ id: 3, amount: 150 });
		expect(result).toEqual([3]);
		u();
	});

	it('reactive sum', () => {
		const table = createReactiveTable([{ id: 1, amount: 100 }], 'id');
		const sum = createQuery(table).sum((r) => r.amount);

		let result: number[] = [];
		const u = sum.subscribe((value) => (result = value));
		expect(result).toEqual([100]);
		u();
	});
});
describe('GroupBy Aggregation', () => {
	it('reactive groupBy with aggregate', () => {
		const table = createReactiveTable(
			[
				{ id: 1, category: 'A', amount: 100 },
				{ id: 2, category: 'B', amount: 200 },
				{ id: 3, category: 'A', amount: 150 }
			],
			'id'
		);

		const grouped = createQuery(table)
			.groupBy((r) => r.category)
			.aggregate((group) => ({
				total: group.sum((r) => r.amount),
				count: group.count()
			}));

		let result: Array<[string, { total: number; count: number }]> = [];
		const unsub = grouped.subscribe((value) => (result = value));

		// Check initial grouping
		expect(result).toHaveLength(2);
		expect(result.find(([key]) => key === 'A')?.[1]).toEqual({ total: 250, count: 2 });
		expect(result.find(([key]) => key === 'B')?.[1]).toEqual({ total: 200, count: 1 });

		// Test reactivity
		table.add({ id: 4, category: 'A', amount: 50 });
		expect(result.find(([key]) => key === 'A')?.[1]).toEqual({ total: 300, count: 3 });
		unsub();
	});
});

describe('AggregateBuilder', () => {
	it('fluent aggregation API', () => {
		const zset = new ZSet([
			[{ price: 10 }, 1],
			[{ price: 20 }, 1]
		]);

		const builder = new AggregateBuilder(zset);
		expect(builder.count()).toBe(2);
		expect(builder.sum((r) => r.price)).toBe(30);
		expect(builder.average((r) => r.price)).toBe(15);
	});
});

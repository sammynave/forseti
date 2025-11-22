import { StatefulTopK } from '$lib/stateful-top-k.js';
import { ZSet, ZSetGroup } from '$lib/z-set.js';
import { describe, bench } from 'vitest';

interface Product {
	id: string;
	name: string;
	price: number;
	category: string;
}
function hashObject(obj: Record<string, any>): string {
	const keys = Object.keys(obj).sort(); // Ensure consistent ordering
	let hash = '';
	for (const key of keys) {
		hash += `${key}:${obj[key]};`;
	}
	return hash;
}

describe('TopK Incremental Processing', () => {
	const products: Product[] = [];

	Array.from({ length: 20 }).forEach((_, idx) => {
		products.push({
			id: `${idx}`,
			name: `name-${idx}`,
			price: idx * 1000,
			category: `cat-${idx}`
		});
	});

	describe('hashing', () => {
		bench('JSON.stringify', () => {
			let map: Map<string, Product> = new Map();
			for (const p of products) {
				const key = JSON.stringify(p);
				map.set(key, p);
			}
		});

		bench('field concat', () => {
			let map: Map<string, Product> = new Map();
			for (const p of products) {
				const key = `${p.id}|${p.name}|${p.price}|${p.category}`;
				map.set(key, p);
			}
		});
		bench('hasObject', () => {
			let map: Map<string, Product> = new Map();
			for (const p of products) {
				const key = hashObject(p);
				map.set(key, p);
			}
		});

		function getObjectKey(obj: object, objectIds: WeakMap<object, string>, nextId: number): string {
			if (objectIds.has(obj)) {
				return objectIds.get(obj)!;
			}
			const id = `obj_${nextId++}`;
			objectIds.set(obj, id);
			return id;
		}
		bench('WeakMap', () => {
			const objectIds = new WeakMap<object, string>();
			let nextId = 0;
			for (const p of products) {
				getObjectKey(p, objectIds, nextId);
				nextId++;
			}
		});
	});

	describe('stateful-top-k', () => {
		// Large dataset to match real bottleneck scenario (10,000 records)
		const largeProductSet: Product[] = [];
		Array.from({ length: 1000 }).forEach((_, idx) => {
			largeProductSet.push({
				id: `${idx}`,
				name: `name-${idx}`,
				price: idx * 1000,
				category: `cat-${idx % 5}` // Some recurring categories
			});
		});

		const comparator = (a: Product, b: Product) => b.price - a.price; // Sort by price desc

		const initialData = new ZSet(largeProductSet.map((p) => [p, 1] as [Product, number]));
		bench('original implementation - 10k records', () => {
			const group = new ZSetGroup<Product>();
			const topK = new StatefulTopK(comparator, Infinity, 0, group);

			// This is the real bottleneck - processing 10,000 records at once
			topK.processIncrement(initialData);
		});

		bench('optimized implementation - 10k records', () => {
			const group = new ZSetGroup<Product>();
			const topK = new StatefulTopK(comparator, Infinity, 0, group);

			topK.processIncrement(initialData);
		});

		bench('optimized with custom key function - 10k records', () => {
			const group = new ZSetGroup<Product>();
			const customKeyFn = (p: Product) => p.id;
			const topK = new StatefulTopK(comparator, Infinity, 0, group, customKeyFn);

			topK.processIncrement(initialData);
		});

		bench('bulk loading with processInitial - 10k records', () => {
			const group = new ZSetGroup<Product>();
			const topK = new StatefulTopK(comparator, Infinity, 0, group);

			topK.processInitial(initialData);
		});

		bench('bulk loading + custom key - 10k records', () => {
			const group = new ZSetGroup<Product>();
			const customKeyFn = (p: Product) => p.id;
			const topK = new StatefulTopK(comparator, Infinity, 0, group, customKeyFn);

			topK.processInitial(initialData);
		});
	});
});

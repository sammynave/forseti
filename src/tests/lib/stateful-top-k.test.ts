import { describe, it, expect, beforeEach } from 'vitest';
import { ZSet, ZSetGroup } from '../../lib/z-set.js';
import { Circuit } from '../../lib/circuit.js';
import { Stream } from '../../lib/stream.js';
import { ZSetOperators } from '../../lib/z-set-operators.js';
import { StatefulTopK } from '../../lib/stateful-top-k.js';

interface Product {
	id: string;
	name: string;
	price: number;
	category: string;
}

describe('TopK Incremental Processing', () => {
	let products: Product[];
	let priceComparator: (a: Product, b: Product) => number;

	beforeEach(() => {
		products = [
			{ id: '1', name: 'Laptop', price: 1000, category: 'electronics' },
			{ id: '2', name: 'Phone', price: 800, category: 'electronics' },
			{ id: '3', name: 'Tablet', price: 600, category: 'electronics' },
			{ id: '4', name: 'Book', price: 20, category: 'books' },
			{ id: '5', name: 'Pen', price: 5, category: 'stationery' }
		];

		priceComparator = (a: Product, b: Product) => b.price - a.price; // Descending
	});

	describe('StatefulTopK basic functionality', () => {
		it('should maintain top-3 products by price', () => {
			const statefulTopK = new StatefulTopK(
				priceComparator,
				3, // limit
				0, // offset
				new ZSetGroup<Product>()
			);

			// Initial batch
			const initial = new ZSet(products.slice(0, 3).map((p) => [p, 1]));
			const result1 = statefulTopK.processIncrement(initial);

			// Should return all 3 (sorted by price desc: Laptop, Phone, Tablet)
			expect(result1.data).toHaveLength(3);
			expect(result1.data[0][0].name).toBe('Laptop');
			expect(result1.data[1][0].name).toBe('Phone');
			expect(result1.data[2][0].name).toBe('Tablet');

			// Add a higher-priced item
			const newProduct = new ZSet([
				[{ id: '6', name: 'Gaming PC', price: 2000, category: 'electronics' }, 1]
			]);
			const result2 = statefulTopK.processIncrement(newProduct);

			// Should evict Tablet (lowest of top-3), add Gaming PC
			const addedItems = result2.data.filter(([_, w]) => w > 0);
			const removedItems = result2.data.filter(([_, w]) => w < 0);

			expect(addedItems).toHaveLength(1);
			expect(addedItems[0][0].name).toBe('Gaming PC');
			expect(removedItems).toHaveLength(1);
			expect(removedItems[0][0].name).toBe('Tablet');
		});

		it('should handle deletions correctly', () => {
			const statefulTopK = new StatefulTopK(priceComparator, 3, 0, new ZSetGroup<Product>());

			// Setup: Top-3 by price
			const initial = new ZSet(products.slice(0, 4).map((p) => [p, 1]));
			statefulTopK.processIncrement(initial);

			// Delete the top item (Laptop)
			const deletion = new ZSet([[products[0], -1]]);
			const result = statefulTopK.processIncrement(deletion);

			// Should remove Laptop and promote Book into top-3
			const addedItems = result.data.filter(([_, w]) => w > 0);
			const removedItems = result.data.filter(([_, w]) => w < 0);

			expect(removedItems[0][0].name).toBe('Laptop');
			expect(addedItems[0][0].name).toBe('Book');
		});

		it('should handle offset correctly', () => {
			const statefulTopK = new StatefulTopK(
				priceComparator,
				2, // limit
				1, // offset (skip top item)
				new ZSetGroup<Product>()
			);

			const initial = new ZSet(products.map((p) => [p, 1]));
			const result = statefulTopK.processIncrement(initial);

			// Should return 2nd and 3rd items (Phone, Tablet)
			expect(result.data).toHaveLength(2);
			expect(result.data[0][0].name).toBe('Phone');
			expect(result.data[1][0].name).toBe('Tablet');
		});
	});

	describe('Circuit.topK incremental behavior', () => {
		it('should match batch processing results', () => {
			const topKCircuit = Circuit.topK<Product>(priceComparator, { limit: 3 });

			// ========== INCREMENTAL COMPUTATION ==========
			const changes = new Stream<ZSet<Product>>(new ZSet([]));
			changes.set(0, new ZSet(products.slice(0, 3).map((p) => [p, 1])));
			changes.set(1, new ZSet([[products[3], 1]])); // Add Book
			changes.set(2, new ZSet([[products[4], 1]])); // Add Pen

			const incrementalResults = topKCircuit.execute(changes);

			// ========== BATCH COMPUTATION ==========
			const step0 = ZSetOperators.topK(
				new ZSet(products.slice(0, 3).map((p) => [p, 1])),
				priceComparator,
				3
			);
			const step1 = ZSetOperators.topK(
				new ZSet(products.slice(0, 4).map((p) => [p, 1])),
				priceComparator,
				3
			);
			const step2 = ZSetOperators.topK(
				new ZSet(products.slice(0, 5).map((p) => [p, 1])),
				priceComparator,
				3
			);

			// Results should be equivalent (though incremental returns deltas)
			// We'd need to integrate the incremental results to compare
			const integratedResult = new ZSetGroup<Product>();
			let accumulated = integratedResult.zero();

			for (const [time, delta] of incrementalResults.entries()) {
				accumulated = integratedResult.add(accumulated, delta);

				// Compare with batch result at each step
				const expectedBatch = time === 0 ? step0 : time === 1 ? step1 : step2;
				expect(accumulated.data.sort()).toEqual(expectedBatch.data.sort());
			}
		});
	});

	describe('Circuit.orderBy composition', () => {
		it('should order products by price with limit', () => {
			const orderByPrice = Circuit.orderBy<Product, number>(
				(p) => p.price,
				(a, b) => b - a, // Descending
				{ limit: 2 }
			);

			const input = new Stream<ZSet<Product>>(new ZSet([]));
			input.set(0, new ZSet(products.map((p) => [p, 1])));

			const result = orderByPrice.execute(input);
			const output = result.at(0);

			expect(output.data).toHaveLength(2);
			expect(output.data[0][0].name).toBe('Laptop'); // Highest price
			expect(output.data[1][0].name).toBe('Phone'); // Second highest
		});

		it('should handle string sorting', () => {
			const orderByName = Circuit.orderBy<Product, string>(
				(p) => p.name,
				(a, b) => a.localeCompare(b), // Ascending alphabetical
				{ limit: 3 }
			);

			const input = new Stream<ZSet<Product>>(new ZSet([]));
			input.set(0, new ZSet(products.map((p) => [p, 1])));

			const result = orderByName.execute(input);
			const output = result.at(0);

			// Should be: Book, Laptop, Pen (alphabetically)
			expect(output.data[0][0].name).toBe('Book');
			expect(output.data[1][0].name).toBe('Laptop');
			expect(output.data[2][0].name).toBe('Pen');
		});
	});

	describe('Performance characteristics', () => {
		it('should process incremental updates efficiently', () => {
			const INITIAL_SIZE = 1000;
			const UPDATE_SIZE = 10;

			// Generate large dataset
			const largeDataset = Array.from({ length: INITIAL_SIZE }, (_, i) => ({
				id: i.toString(),
				name: `Product${i}`,
				price: Math.random() * 1000,
				category: 'test'
			}));

			const statefulTopK = new StatefulTopK(
				priceComparator,
				10, // Only track top-10
				0,
				new ZSetGroup<Product>()
			);

			// Process initial batch
			const initial = new ZSet(largeDataset.map((p) => [p, 1]));
			const startTime = performance.now();
			statefulTopK.processIncrement(initial);
			const initialTime = performance.now() - startTime;

			// Process small update
			const updates = largeDataset
				.slice(0, UPDATE_SIZE)
				.map((p) => ({ ...p, price: p.price + 100 }));
			const updateDelta = new ZSet([
				...largeDataset.slice(0, UPDATE_SIZE).map((p) => [p, -1]), // Remove old
				...updates.map((p) => [p, 1]) // Add updated
			]);

			const updateStartTime = performance.now();
			const result = statefulTopK.processIncrement(updateDelta);
			const updateTime = performance.now() - updateStartTime;

			// Update should be much faster than initial (though this is a rough test)
			console.log(`Initial: ${initialTime}ms, Update: ${updateTime}ms`);
			expect(updateTime).toBeLessThan(initialTime * 0.5); // Update should be < 50% of initial
			expect(result.data.length).toBeLessThanOrEqual(20); // At most 10 additions + 10 removals
		});
	});
});

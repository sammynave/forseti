import { bench, describe, beforeEach, expect } from 'vitest';
import { EventStore, todoCreatedEvent, todoToggledEvent } from './event-store.js';
import { Query } from './query-builder.js';
import { ZSet } from './z-set.js';
import { Stream } from './stream.js';

type Todo = { id: string; title: string; done: boolean };

// Helper function to create large initial datasets
function createLargeDataset(size: number): EventStore {
	const eventStore = new EventStore();

	for (let i = 0; i < size; i++) {
		const isCompleted = i % 3 === 0;
		eventStore.append(todoCreatedEvent({ title: `Todo ${i}` }));

		if (isCompleted) {
			const todos = eventStore.getTodos();
			const lastTodo = todos[todos.length - 1];
			eventStore.append(todoToggledEvent({ id: lastTodo.id }));
		}
	}

	return eventStore;
}

describe(`Simple comparison`, () => {
	// Test different dataset sizes to prove scaling
	const datasetSize = 100;
	const eventStore = createLargeDataset(datasetSize);
	const initialSnapshot = eventStore.getCurrentSnapshot();

	eventStore.append(todoCreatedEvent({ title: 'New Todo' }));
	const processor = Query.from<Todo>()
		.where((todo: Todo) => todo.done)
		.createStreamingProcessor(initialSnapshot);

	// Create a small change
	const change = new ZSet();
	change.add({ id: 'test', title: 'New Todo', done: true }, 1);
	bench(`Full Recomputation - ${datasetSize} todos`, () => {
		// Full recomputation: rebuild entire filtered view
		eventStore
			.getCurrentSnapshot()
			.plus(change)
			.materialize.filter((todo) => todo.done);
	});

	bench(`Incremental Recomputation - ${datasetSize} todos`, () => {
		// Incremental: process change and get final state
		processor.processChange(change);
		processor.getCurrentState().materialize;
	});
});

describe.only(`Complex comparison`, () => {
	const datasetSize = 1000;

	// === E-commerce Data Streams Setup ===
	// Users stream (scaled to datasetSize)
	const usersStream = new Stream();
	const usersZSet = new ZSet();
	for (let i = 1; i <= datasetSize; i++) {
		const tier = i % 3 === 0 ? 'premium' : 'basic';
		const totalSpent = 500 + i * 10 + (tier === 'premium' ? 500 : 0);
		usersZSet.add({ userId: i, name: `User${i}`, tier, totalSpent }, 1);
	}
	usersStream.append(usersZSet);

	// Orders stream
	const ordersStream = new Stream();
	const ordersZSet = new ZSet();
	for (let i = 1; i <= datasetSize * 2; i++) {
		const userId = (i % datasetSize) + 1;
		const productId = (i % 10) + 201; // 10 different products
		ordersZSet.add({ orderId: 100 + i, userId, productId, amount: 200 + (i % 500) }, 1);
	}
	ordersStream.append(ordersZSet);

	// Products stream
	const productsStream = new Stream();
	const productsZSet = new ZSet();
	for (let i = 201; i <= 210; i++) {
		const categoryId = i <= 205 ? 1 : 2; // Electronics vs Books
		const name = categoryId === 1 ? `Electronics${i}` : `Book${i}`;
		productsZSet.add({ productId: i, name, categoryId, price: 300 + (i % 200) }, 1);
	}
	productsStream.append(productsZSet);

	// Categories stream
	const categoriesStream = new Stream();
	const categoriesZSet = new ZSet();
	categoriesZSet.add({ categoryId: 1, name: 'Electronics' }, 1);
	categoriesZSet.add({ categoryId: 2, name: 'Books' }, 1);
	categoriesStream.append(categoriesZSet);

	// Reviews stream
	const reviewsStream = new Stream();
	const reviewsZSet = new ZSet();
	for (let i = 1; i <= datasetSize; i++) {
		const userId = i;
		const productId = 201 + (i % 10);
		const rating = 3 + (i % 3); // Ratings 3, 4, 5
		reviewsZSet.add({ userId, productId, rating, comment: `Review${i}` }, 1);
	}
	reviewsStream.append(reviewsZSet);

	// === Complex Master Query ===
	const premiumElectronicsReviewersQuery = Query.from(usersStream)
		.where((user) => user.tier === 'premium')
		.join(
			ordersStream,
			(user) => user.userId,
			(order) => order.userId
		)
		.select(([user, order]) => ({
			userId: user.userId,
			name: user.name,
			tier: user.tier,
			totalSpent: user.totalSpent,
			orderId: order.orderId,
			productId: order.productId,
			amount: order.amount
		}))
		.join(
			productsStream,
			(item) => item.productId,
			(product) => product.productId
		)
		.select(([item, product]) => ({
			...item,
			productName: product.name,
			categoryId: product.categoryId,
			price: product.price
		}))
		.join(
			categoriesStream,
			(item) => item.categoryId,
			(category) => category.categoryId
		)
		.where(([item, category]) => category.name === 'Electronics')
		.select(([item, category]) => item)
		.join(
			reviewsStream,
			(item) => `${item.userId}_${item.productId}`,
			(review) => `${review.userId}_${review.productId}`
		)
		.where(([item, review]) => review.rating >= 4)
		.select(([item, review]) => ({
			userId: item.userId,
			userName: item.name,
			tier: item.tier,
			totalSpent: item.totalSpent,
			category: 'Electronics',
			rating: review.rating
		}))
		.distinct();

	const highSpendersQuery = Query.from(usersStream)
		.where((user) => user.totalSpent >= 1000)
		.select((user) => ({
			userId: user.userId,
			userName: user.name,
			tier: user.tier,
			totalSpent: user.totalSpent,
			category: 'HighSpender',
			rating: null
		}))
		.distinct();

	const masterQuery = premiumElectronicsReviewersQuery
		.union(highSpendersQuery)
		.distinct()
		.where((item) => item.totalSpent > 500)
		.select((item) => ({
			analyticsId: `${item.category}_${item.userId}`,
			profile: { name: item.userName, tier: item.tier, spending: item.totalSpent },
			segment: item.category === 'Electronics' ? 'tech-savvy-premium' : 'high-value',
			qualityScore: item.rating || 'N/A'
		}));

	// === Setup for benchmarks ===
	const initialSnapshot = usersStream.get(0);
	const processor = masterQuery.createStreamingProcessor(initialSnapshot);

	// Test changes: Diana + her orders + reviews (coordinated across all streams)
	const userChange = new ZSet();
	userChange.add({ userId: datasetSize + 1, name: 'Diana', tier: 'basic', totalSpent: 2000 }, 1);

	const orderChange = new ZSet();
	orderChange.add({ orderId: 999, userId: datasetSize + 1, productId: 201, amount: 600 }, 1);

	const reviewChange = new ZSet();
	reviewChange.add(
		{ userId: datasetSize + 1, productId: 201, rating: 5, comment: 'Excellent!' },
		1
	);

	// Create complete updated snapshots for all streams
	const fullUsersSnapshot = usersStream.get(0).plus(userChange);
	const fullOrdersSnapshot = ordersStream.get(0).plus(orderChange);
	const fullReviewsSnapshot = reviewsStream.get(0).plus(reviewChange);

	let count = 0;
	bench(`Full Recomputation - ${datasetSize} users`, () => {
		// Create fresh streams with updated data
		const fullUsersStream = new Stream();
		const fullOrdersStream = new Stream();
		const fullReviewsStream = new Stream();

		fullUsersStream.append(fullUsersSnapshot);
		fullOrdersStream.append(fullOrdersSnapshot);
		fullReviewsStream.append(fullReviewsSnapshot);

		// Rebuild entire complex query from scratch
		const fullPremiumElectronicsQuery = Query.from(fullUsersStream)
			.where((user) => user.tier === 'premium')
			.join(
				ordersStream,
				(user) => user.userId,
				(order) => order.userId
			)
			.select(([user, order]) => ({
				/* full select */
			}))
			.join(
				productsStream,
				(item) => item.productId,
				(product) => product.productId
			)
			.select(([item, product]) => ({
				/* full select */
			}))
			.join(
				categoriesStream,
				(item) => item.categoryId,
				(category) => category.categoryId
			)
			.where(([item, category]) => category.name === 'Electronics')
			.select(([item, category]) => item)
			.join(
				reviewsStream,
				(item) => `${item.userId}_${item.productId}`,
				(review) => `${review.userId}_${review.productId}`
			)
			.where(([item, review]) => review.rating >= 4)
			.select(([item, review]) => ({
				/* full select */
			}))
			.distinct();

		const fullHighSpendersQuery = Query.from(fullUsersStream)
			.where((user) => user.totalSpent >= 1000)
			.select((user) => ({
				/* full select */
			}))
			.distinct();

		const fullMasterQuery = fullPremiumElectronicsQuery
			.union(fullHighSpendersQuery)
			.distinct()
			.where((item) => item.totalSpent > 500)
			.select((item) => ({
				/* final projection */
			}));

		const fullProcessor = fullMasterQuery.createStreamingProcessor(fullUsersSnapshot);
		const x = fullProcessor.getCurrentState().materialize;
		// if (count === 1) {
		// 	console.log('f', JSON.stringify(x));
		// }
		// count++;
	});

	let count1 = 0;
	bench(`Incremental Recomputation - ${datasetSize} users`, () => {
		processor.processChange(userChange);
		const x = processor.getCurrentState().materialize;
		// if (count1 === 1) {
		// 	console.log('i', JSON.stringify(x));
		// }
		// count1++;
	});
});

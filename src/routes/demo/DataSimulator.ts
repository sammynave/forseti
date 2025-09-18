import type { User, Product, Order } from './types.js';

export class DataSimulator {
	private static readonly COUNTRIES = [
		'USA',
		'Canada',
		'UK',
		'Germany',
		'France',
		'Japan',
		'Australia',
		'Brazil',
		'India',
		'China',
		'Spain',
		'Italy',
		'Netherlands',
		'Sweden'
	];

	private static readonly PRODUCT_CATEGORIES = [
		'Electronics',
		'Clothing',
		'Home & Garden',
		'Sports',
		'Books',
		'Beauty',
		'Automotive',
		'Toys',
		'Food',
		'Health'
	];

	private static readonly PRODUCT_NAMES = {
		Electronics: ['Laptop', 'Smartphone', 'Headphones', 'Tablet', 'Smart Watch', 'Camera'],
		Clothing: ['T-Shirt', 'Jeans', 'Sneakers', 'Jacket', 'Dress', 'Hoodie'],
		'Home & Garden': ['Coffee Maker', 'Lamp', 'Chair', 'Plant Pot', 'Blanket', 'Candle'],
		Sports: ['Running Shoes', 'Yoga Mat', 'Dumbbells', 'Basketball', 'Bike', 'Protein Powder'],
		Books: ['Novel', 'Cookbook', 'Biography', 'Textbook', 'Comics', 'Self-Help'],
		Beauty: ['Lipstick', 'Shampoo', 'Face Cream', 'Perfume', 'Nail Polish', 'Mascara'],
		Automotive: ['Car Charger', 'Phone Mount', 'Tire Gauge', 'Air Freshener', 'Dashboard Cam'],
		Toys: ['Board Game', 'Action Figure', 'Puzzle', 'Building Blocks', 'Doll', 'Remote Car'],
		Food: ['Protein Bar', 'Coffee Beans', 'Snack Mix', 'Honey', 'Olive Oil', 'Hot Sauce'],
		Health: [
			'Vitamins',
			'First Aid Kit',
			'Thermometer',
			'Hand Sanitizer',
			'Bandages',
			'Pain Relief'
		]
	};

	private orderIdCounter = 1;

	generateUsers(count: number): User[] {
		const users: User[] = [];

		for (let i = 0; i < count; i++) {
			users.push({
				id: `user_${i + 1}`,
				country: this.randomChoice(DataSimulator.COUNTRIES),
				signupDate: Date.now() - Math.random() * 365 * 24 * 60 * 60 * 1000, // Random date within last year
				tier: this.weightedRandomChoice([
					{ value: 'basic', weight: 0.7 },
					{ value: 'premium', weight: 0.25 },
					{ value: 'enterprise', weight: 0.05 }
				])
			});
		}

		return users;
	}

	generateProducts(count: number): Product[] {
		const products: Product[] = [];

		for (let i = 0; i < count; i++) {
			const category = this.randomChoice(DataSimulator.PRODUCT_CATEGORIES);
			const names =
				DataSimulator.PRODUCT_NAMES[category as keyof typeof DataSimulator.PRODUCT_NAMES];
			const name = this.randomChoice(names);

			products.push({
				id: `product_${i + 1}`,
				category,
				name: `${name} ${this.randomVariant()}`,
				price: this.randomPrice(category),
				inventory: Math.floor(Math.random() * 1000) + 50
			});
		}

		return products;
	}

	generateInitialOrders(count: number, users: User[], products: Product[]): Order[] {
		const orders: Order[] = [];
		const now = Date.now();

		for (let i = 0; i < count; i++) {
			const user = this.randomChoice(users);
			const product = this.randomChoice(products);

			orders.push({
				id: `order_${this.orderIdCounter++}`,
				userId: user.id,
				productId: product.id,
				amount: this.calculateOrderAmount(product.price, user.tier),
				timestamp: now - Math.random() * 30 * 24 * 60 * 60 * 1000 // Random within last 30 days
			});
		}

		return orders.sort((a, b) => a.timestamp - b.timestamp);
	}

	generateNewOrders(count: number, users: User[], products: Product[]): Order[] {
		const orders: Order[] = [];
		const now = Date.now();

		for (let i = 0; i < count; i++) {
			const user = this.randomChoice(users);
			const product = this.randomChoice(products);

			orders.push({
				id: `order_${this.orderIdCounter++}`,
				userId: user.id,
				productId: product.id,
				amount: this.calculateOrderAmount(product.price, user.tier),
				timestamp: now - Math.random() * 1000 // Recent orders (within last second)
			});
		}

		return orders;
	}

	private randomChoice<T>(array: T[]): T {
		return array[Math.floor(Math.random() * array.length)];
	}

	private weightedRandomChoice<T>(choices: { value: T; weight: number }[]): T {
		const totalWeight = choices.reduce((sum, choice) => sum + choice.weight, 0);
		let random = Math.random() * totalWeight;

		for (const choice of choices) {
			random -= choice.weight;
			if (random <= 0) {
				return choice.value;
			}
		}

		return choices[choices.length - 1].value;
	}

	private randomVariant(): string {
		const variants = [
			'Pro',
			'Plus',
			'Deluxe',
			'Premium',
			'Standard',
			'Mini',
			'Max',
			'Elite',
			'Classic'
		];
		return this.randomChoice(variants);
	}

	private randomPrice(category: string): number {
		const priceRanges = {
			Electronics: [50, 2000],
			Clothing: [15, 200],
			'Home & Garden': [10, 300],
			Sports: [20, 500],
			Books: [8, 50],
			Beauty: [5, 150],
			Automotive: [10, 200],
			Toys: [5, 100],
			Food: [3, 50],
			Health: [5, 100]
		};

		const [min, max] = priceRanges[category as keyof typeof priceRanges] || [10, 100];
		return Math.floor(Math.random() * (max - min) + min);
	}

	private calculateOrderAmount(basePrice: number, tier: string): number {
		// Apply tier-based discounts/premiums
		let multiplier = 1;
		switch (tier) {
			case 'premium':
				multiplier = 0.9; // 10% discount
				break;
			case 'enterprise':
				multiplier = 0.8; // 20% discount
				break;
		}

		// Add some random quantity (1-3 items typically)
		const quantity = Math.floor(Math.random() * 3) + 1;

		return Math.round(basePrice * multiplier * quantity * 100) / 100;
	}
}

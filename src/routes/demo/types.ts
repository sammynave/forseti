export interface User {
	id: string;
	country: string;
	signupDate: number;
	tier: 'basic' | 'premium' | 'enterprise';
}

export interface Product {
	id: string;
	category: string;
	name: string;
	price: number;
	inventory: number;
}

export interface Order {
	id: string;
	userId: string;
	productId: string;
	amount: number;
	timestamp: number;
}

export interface PerformanceMetrics {
	updateTime: number;
	queryTime: number;
	totalMemory: number;
}

export interface RevenueByCountry {
	country: string;
	revenue: number;
	orderCount: number;
}

export interface Order {
	id: number;
	userId: string;
	amount: number;
}

export interface User {
	id: string;
	name: string;
	age: number;
}

// Generate test data
export function generateOrders(count: number, userIds: string[]): Order[] {
	return Array.from({ length: count }, (_, i) => ({
		id: i,
		userId: userIds[i % userIds.length],
		amount: Math.floor(Math.random() * 1000) + 1
	}));
}

export function generateUsers(count: number): User[] {
	return Array.from({ length: count }, (_, i) => ({
		id: `user${i}`,
		name: `User ${i}`,
		age: Math.floor(Math.random() * 50) + 18
	}));
}

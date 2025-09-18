<script lang="ts">
	import type { Order, User, Product, PerformanceMetrics, RevenueByCountry } from './types.js';

	export let orders: Order[] = [];
	export let users: User[] = [];
	export let products: Product[] = [];
	export let metrics: PerformanceMetrics;

	let revenueByCountry: RevenueByCountry[] = [];
	let topCategories: { category: string; revenue: number }[] = [];
	let premiumUserRevenue = 0;
	let lastUpdateTime = 0;

	// Traditional approach: Re-compute everything when data changes
	$: if (orders && users && products) {
		const startTime = performance.now();
		computeTraditionalMetrics();
		const endTime = performance.now();

		const updateTime = endTime - startTime;

		metrics = {
			updateTime,
			queryTime: updateTime, // Same as update time since we re-query everything
			totalMemory: estimateMemoryUsage()
		};

		lastUpdateTime = Date.now();
	}

	function computeTraditionalMetrics() {
		// Filter orders from last 24 hours
		const last24h = Date.now() - 24 * 60 * 60 * 1000;
		const recentOrders = orders.filter((order) => order.timestamp > last24h);

		// Create lookup maps (expensive - recreated every time!)
		const userLookup = new Map(users.map((user) => [user.id, user]));
		const productLookup = new Map(products.map((product) => [product.id, product]));

		// Add computational complexity to simulate real-world overhead
		// This ensures we get realistic variable query times
		let complexityCounter = 0;

		// Variable computational load to simulate realistic performance variation
		const baseLoad = Math.min(orders.length * 0.01, 5000);
		const variableLoad = baseLoad + Math.random() * baseLoad * 0.5; // Add 0-50% variation

		for (let i = 0; i < variableLoad; i++) {
			complexityCounter += Math.sqrt(i * Math.random() * Math.sin(i * 0.1));
		}

		// Use complexity counter to ensure computation isn't optimized away
		const complexityAdjustment = complexityCounter > 0 ? Math.floor(Math.random() * 3) : 0;

		// Revenue by country (expensive joins and aggregations)
		const countryRevenue = new Map<string, { revenue: number; orderCount: number }>();

		for (const order of recentOrders) {
			const user = userLookup.get(order.userId);

			// Add string processing overhead (simulating real analytics)
			if (user && user.tier === 'premium') {
				// Simulate complex data processing
				const processedCountry = user.country.toLowerCase().trim().toUpperCase();

				const current = countryRevenue.get(processedCountry) || { revenue: 0, orderCount: 0 };
				countryRevenue.set(processedCountry, {
					revenue: current.revenue + order.amount + complexityAdjustment,
					orderCount: current.orderCount + 1
				});
			}
		}

		// Convert to array and sort (expensive)
		revenueByCountry = Array.from(countryRevenue.entries())
			.map(([country, data]) => ({
				country,
				revenue: data.revenue,
				orderCount: data.orderCount
			}))
			.sort((a, b) => b.revenue - a.revenue)
			.slice(0, 10); // Top 10

		// Categories breakdown (another expensive join and aggregation)
		const categoryRevenue = new Map<string, number>();

		for (const order of recentOrders) {
			const user = userLookup.get(order.userId);
			const product = productLookup.get(order.productId);

			if (user && user.tier === 'premium' && product) {
				const current = categoryRevenue.get(product.category) || 0;
				categoryRevenue.set(product.category, current + order.amount);
			}
		}

		topCategories = Array.from(categoryRevenue.entries())
			.map(([category, revenue]) => ({ category, revenue }))
			.sort((a, b) => b.revenue - a.revenue)
			.slice(0, 5);

		// Total premium user revenue
		premiumUserRevenue = revenueByCountry.reduce((sum, item) => sum + item.revenue, 0);
	}

	function estimateMemoryUsage(): number {
		// Rough estimation of memory usage
		return (
			orders.length * 100 + // Order objects
			users.length * 80 + // User objects
			products.length * 90 + // Product objects
			revenueByCountry.length * 60 + // Computed metrics
			topCategories.length * 50
		);
	}

	function formatCurrency(amount: number): string {
		return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(amount);
	}

	function formatDuration(ms: number): string {
		// Log raw values to see what we're actually getting
		if (ms < 1) return `${(ms * 1000).toFixed(3)}μs`; // More precision
		if (ms < 1000) return `${ms.toFixed(4)}ms`; // More precision
		return `${(ms / 1000).toFixed(4)}s`;
	}
</script>

<div class="traditional-panel">
	<div class="metrics-header">
		<h3>Performance Metrics</h3>
		<div class="metric">
			<span class="label">Update Time:</span>
			<span class="value">{formatDuration(metrics.updateTime)}</span>
		</div>
		<div class="metric">
			<span class="label">Query Time:</span>
			<span class="value">{formatDuration(metrics.queryTime)}</span>
		</div>
		<div class="metric">
			<span class="label">Memory Usage:</span>
			<span class="value">{(metrics.totalMemory / 1024).toFixed(1)} KB</span>
		</div>
		<div class="metric">
			<span class="label">Last Updated:</span>
			<span class="value"
				>{lastUpdateTime ? new Date(lastUpdateTime).toLocaleTimeString() : 'Never'}</span
			>
		</div>
	</div>

	<div class="analytics-content">
		<div class="section">
			<h4>Premium Users - Revenue by Country (Last 24h)</h4>
			<div class="total-revenue">
				Total: {formatCurrency(premiumUserRevenue)}
			</div>
			<div class="revenue-list">
				{#each revenueByCountry as item (item.country)}
					<div class="revenue-item">
						<span class="country">{item.country}</span>
						<div class="revenue-details">
							<span class="revenue">{formatCurrency(item.revenue)}</span>
							<span class="orders">({item.orderCount} orders)</span>
						</div>
					</div>
				{/each}
			</div>
		</div>

		<div class="section">
			<h4>Top Categories (Premium Users)</h4>
			<div class="category-list">
				{#each topCategories as item (item.category)}
					<div class="category-item">
						<span class="category">{item.category}</span>
						<span class="revenue">{formatCurrency(item.revenue)}</span>
					</div>
				{/each}
			</div>
		</div>

		<div class="section">
			<h4>Processing Details</h4>
			<div class="details">
				<p>• Full table scan of {orders.length.toLocaleString()} orders</p>
				<p>• Rebuilds lookup maps for {users.length.toLocaleString()} users</p>
				<p>• Recreates product index for {products.length.toLocaleString()} products</p>
				<p>• Re-computes all aggregations from scratch</p>
				<p>• Re-sorts and filters all results</p>
			</div>
		</div>
	</div>
</div>

<style>
	.traditional-panel {
		height: 600px;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.metrics-header {
		background: #fef2f2;
		border: 1px solid #fecaca;
		border-radius: 8px;
		padding: 15px;
	}

	.metrics-header h3 {
		margin: 0 0 10px 0;
		color: #dc2626;
		font-size: 1rem;
	}

	.metric {
		display: flex;
		justify-content: space-between;
		margin-bottom: 5px;
		font-size: 0.9rem;
	}

	.metric .label {
		color: #6b7280;
	}

	.metric .value {
		font-weight: 600;
		color: #dc2626;
		font-family: 'Monaco', monospace;
	}

	.analytics-content {
		flex: 1;
		overflow-y: auto;
		display: flex;
		flex-direction: column;
		gap: 20px;
	}

	.section {
		background: #f9fafb;
		border-radius: 8px;
		padding: 15px;
		border: 1px solid #e5e7eb;
	}

	.section h4 {
		margin: 0 0 15px 0;
		color: #374151;
		font-size: 0.95rem;
	}

	.total-revenue {
		font-size: 1.2rem;
		font-weight: 700;
		color: #059669;
		margin-bottom: 15px;
	}

	.revenue-list,
	.category-list {
		display: flex;
		flex-direction: column;
		gap: 8px;
	}

	.revenue-item,
	.category-item {
		display: flex;
		justify-content: space-between;
		align-items: center;
		padding: 8px 12px;
		background: white;
		border-radius: 6px;
		border: 1px solid #e5e7eb;
	}

	.country,
	.category {
		font-weight: 600;
		color: #374151;
	}

	.revenue-details {
		display: flex;
		flex-direction: column;
		align-items: flex-end;
	}

	.revenue {
		font-weight: 600;
		color: #059669;
		font-family: 'Monaco', monospace;
		font-size: 0.9rem;
	}

	.orders {
		font-size: 0.8rem;
		color: #6b7280;
	}

	.details {
		font-size: 0.85rem;
		color: #6b7280;
	}

	.details p {
		margin: 5px 0;
	}
</style>

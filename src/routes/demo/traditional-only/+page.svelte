<script lang="ts">
	import { onMount, onDestroy } from 'svelte';
	import TraditionalPanel from '../TraditionalPanel.svelte';
	import PerformanceMetrics from '../PerformanceMetrics.svelte';
	import { DataSimulator } from '../DataSimulator.js';
	import type { Order, User, Product } from '../types.js';

	let dataSimulator: DataSimulator;
	let isRunning = false;
	let animationFrameId: number;

	// Shared data state
	let orders: Order[] = [];
	let users: User[] = [];
	let products: Product[] = [];

	// Performance tracking
	let traditionalMetrics = { updateTime: 0, queryTime: 0, totalMemory: 0 };
	let ivmMetrics = { updateTime: 0, queryTime: 0, totalMemory: 0 };

	// Demo modes
	let currentMode: 'normal' | 'stress' = 'normal';
	let isLoading = false;
	let resetSignal = 0;

	const DEMO_CONFIGS = {
		normal: {
			users: 5000,
			products: 1000,
			orders: 15000,
			ordersPerSecond: 20, // ~20 orders per second
			maxOrdersPerFrame: 2,
			name: 'Normal Demo'
		},
		stress: {
			users: 800000,
			products: 800000,
			orders: 900000,
			ordersPerSecond: 1000, // ~100 orders per second
			maxOrdersPerFrame: 1000,
			name: 'Stress Test (1+ sec traditional queries)'
		}
	};

	onMount(() => {
		dataSimulator = new DataSimulator();
		loadDemoData('stress');
	});

	async function loadDemoData(mode: 'normal' | 'stress') {
		isLoading = true;
		stopSimulation();

		const config = DEMO_CONFIGS[mode];

		// Show progress for large datasets
		if (mode === 'stress') {
			console.log('Loading stress test data...');
		}

		users = dataSimulator.generateUsers(config.users);
		products = dataSimulator.generateProducts(config.products);
		orders = dataSimulator.generateInitialOrders(config.orders, users, products);

		currentMode = mode;
		resetSignal++; // Signal IVM to reset for new dataset
		isLoading = false;
	}

	// RAF timing variables
	let lastFrameTime = 0;
	let accumulatedTime = 0;

	function startSimulation() {
		if (isRunning) return;

		isRunning = true;
		lastFrameTime = performance.now();
		accumulatedTime = 0;
		animationLoop();
	}

	function animationLoop() {
		if (!isRunning) return;

		const currentTime = performance.now();
		const deltaTime = currentTime - lastFrameTime;
		lastFrameTime = currentTime;

		accumulatedTime += deltaTime;

		const config = DEMO_CONFIGS[currentMode];
		const targetFrameTime = 1000 / config.ordersPerSecond; // Time per order in ms

		// Generate orders based on accumulated time and target rate
		if (accumulatedTime >= targetFrameTime) {
			const ordersToGenerate = Math.min(
				Math.floor(accumulatedTime / targetFrameTime),
				config.maxOrdersPerFrame
			);

			if (ordersToGenerate > 0) {
				const newOrders = dataSimulator.generateNewOrders(ordersToGenerate, users, products);
				orders = [...orders, ...newOrders];
				accumulatedTime -= ordersToGenerate * targetFrameTime;
			}
		}

		animationFrameId = requestAnimationFrame(animationLoop);
	}

	function stopSimulation() {
		isRunning = false;
		if (animationFrameId) {
			cancelAnimationFrame(animationFrameId);
		}
	}

	function resetData() {
		stopSimulation();
		const config = DEMO_CONFIGS[currentMode];
		orders = dataSimulator.generateInitialOrders(config.orders, users, products);
		resetSignal++; // Signal IVM to reset
	}

	onDestroy(() => {
		stopSimulation();
	});

	// Calculate key metrics for display
	$: totalRevenue = orders.reduce((sum, order) => sum + order.amount, 0);
	$: ordersLast24h = orders.filter(
		(order) => Date.now() - order.timestamp < 24 * 60 * 60 * 1000
	).length;
</script>

<div class="demo-container">
	<header class="demo-header">
		<h1>Traditional Querying Demo</h1>
		<p>Real-time E-commerce Analytics Dashboard</p>

		<div class="controls">
			<div class="mode-selector">
				<button
					class="mode-btn"
					class:active={currentMode === 'normal'}
					on:click={() => loadDemoData('normal')}
					disabled={isLoading}
				>
					Normal Mode
				</button>
				<button
					class="mode-btn stress-mode"
					class:active={currentMode === 'stress'}
					on:click={() => loadDemoData('stress')}
					disabled={isLoading}
				>
					🚀 Stress Test
				</button>
			</div>

			<div class="simulation-controls">
				<button on:click={startSimulation} disabled={isRunning || isLoading}>
					Start Simulation
				</button>
				<button on:click={stopSimulation} disabled={!isRunning}> Stop Simulation </button>
				<button on:click={resetData} disabled={isLoading}>Reset Data</button>
			</div>

			<div class="status">
				{#if isLoading}
					⏳ Loading {DEMO_CONFIGS[currentMode].name}...
				{:else}
					{isRunning ? '🟢 Simulating' : '🔴 Stopped'} |
					{DEMO_CONFIGS[currentMode].name} |
					{orders.length.toLocaleString()} total orders |
					{ordersLast24h.toLocaleString()} orders (24h) | ${totalRevenue.toLocaleString()} total revenue
				{/if}
			</div>
		</div>
	</header>

	<div class="panels-container">
		<!-- Traditional Approach Panel -->
		<div class="panel">
			<h2>🐌 Traditional Approach</h2>
			<p>Re-computes entire query on every update</p>
			<p>
				NOTE: having this going at the same time as IVM slows the IVM updates. maybe hide one then
				the other or compare in two tabs or use web workers
			</p>
			<TraditionalPanel {orders} {users} {products} bind:metrics={traditionalMetrics} />
		</div>

		<!-- IVM Approach Panel -->
		<div class="panel">
			<h2>⚡ IVM Approach</h2>
			<p>Processes only incremental changes</p>
			<p>
				NOTE: if you look at the flame graph in the chrome devtools Performance tab, you can see
				that the incremental update is ~.1 ms or 100 microsends. that's faster than the
				`fromatCurrency` call which is about ~.3ms!
			</p>
		</div>
	</div>

	<!-- Performance Comparison -->
	<PerformanceMetrics {traditionalMetrics} {ivmMetrics} />
</div>

<style>
	.demo-container {
		max-width: 1400px;
		margin: 0 auto;
		padding: 20px;
		font-family: 'Inter', sans-serif;
	}

	.demo-header {
		text-align: center;
		margin-bottom: 30px;
		padding-bottom: 20px;
		border-bottom: 2px solid #e5e7eb;
	}

	.demo-header h1 {
		font-size: 2.5rem;
		font-weight: 700;
		color: #1f2937;
		margin-bottom: 10px;
	}

	.demo-header p {
		font-size: 1.2rem;
		color: #6b7280;
		margin-bottom: 20px;
	}

	.controls {
		display: flex;
		gap: 20px;
		align-items: center;
		justify-content: center;
		flex-wrap: wrap;
	}

	.mode-selector {
		display: flex;
		gap: 8px;
		background: #f3f4f6;
		padding: 4px;
		border-radius: 12px;
		border: 1px solid #d1d5db;
	}

	.mode-btn {
		px: 16px;
		py: 8px;
		font-weight: 500;
		border-radius: 8px;
		border: none;
		cursor: pointer;
		transition: all 0.2s;
		background: transparent;
		color: #6b7280;
	}

	.mode-btn.active {
		background: #3b82f6;
		color: white;
		box-shadow: 0 2px 4px rgba(59, 130, 246, 0.3);
	}

	.mode-btn.stress-mode.active {
		background: #dc2626;
		box-shadow: 0 2px 4px rgba(220, 38, 38, 0.3);
	}

	.mode-btn:not(:disabled):hover {
		background: #e5e7eb;
	}

	.mode-btn.active:hover {
		background: #2563eb;
	}

	.mode-btn.stress-mode.active:hover {
		background: #b91c1c;
	}

	.simulation-controls {
		display: flex;
		gap: 10px;
	}

	.simulation-controls button {
		px: 20px;
		py: 10px;
		font-weight: 600;
		border-radius: 8px;
		border: none;
		cursor: pointer;
		transition: all 0.2s;
	}

	.simulation-controls button:not(:disabled) {
		background: #3b82f6;
		color: white;
	}

	.simulation-controls button:not(:disabled):hover {
		background: #2563eb;
		transform: translateY(-1px);
	}

	.simulation-controls button:disabled {
		background: #e5e7eb;
		color: #9ca3af;
		cursor: not-allowed;
	}

	.status {
		font-family: 'Monaco', monospace;
		font-size: 0.9rem;
		color: #374151;
		background: #f3f4f6;
		padding: 8px 16px;
		border-radius: 6px;
		border: 1px solid #d1d5db;
	}

	.panels-container {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 30px;
		margin-bottom: 30px;
	}

	.panel {
		background: white;
		border-radius: 12px;
		padding: 25px;
		box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
		border: 1px solid #e5e7eb;
	}

	.panel h2 {
		font-size: 1.5rem;
		font-weight: 600;
		margin-bottom: 8px;
	}

	.panel p {
		color: #6b7280;
		margin-bottom: 20px;
		font-size: 0.95rem;
	}

	@media (max-width: 768px) {
		.panels-container {
			grid-template-columns: 1fr;
		}

		.controls {
			flex-direction: column;
			gap: 10px;
		}
	}
</style>

<script>
	import { createStatefulJoinCircuit } from '$lib/stateful-circuit.js';
	import { ZSet } from '$lib/z-set.js';

	// Types
	// type Order = { id: number; userId: string; amount: number };
	// type User = { id: string; name: string; age: number };

	// Reactive state using $state (Svelte 5)
	let tableRows = $state([]);
	let sorted = $derived(tableRows.toSorted((a, b) => a.orderId - b.orderId));
	$inspect('sorted', sorted);

	// Create stateful circuit once (outside reactive context)
	const circuit = createStatefulJoinCircuit(
		(order) => order.userId,
		(user) => user.id
	);

	// Initialize with base data
	const initialOrders = [
		{ id: 1, userId: 'alice', amount: 100 },
		{ id: 2, userId: 'bob', amount: 200 }
	];

	const initialUsers = [
		{ id: 'alice', name: 'Alice', age: 25 },
		{ id: 'bob', name: 'Bob', age: 30 }
	];

	circuit.initialize(
		new ZSet(initialOrders.map((o) => [o, 1])),
		new ZSet(initialUsers.map((u) => [u, 1]))
	);

	// Set initial table from materialized view
	tableRows = circuit.getMaterializedView().data.map(([[order, user]]) => ({
		orderId: order.id,
		userId: user.id,
		userName: user.name,
		amount: order.amount
	}));

	// Function to process new data with surgical updates
	function addNewOrder(order) {
		// Process the delta
		const delta = circuit.processIncrement(new ZSet([[order, 1]]), new ZSet([]));

		// Apply delta surgically to table
		for (const [[order, user], weight] of delta.data) {
			const row = {
				orderId: order.id,
				userId: user.id,
				userName: user.name,
				amount: order.amount
			};

			if (weight > 0) {
				tableRows.push(row); // Add row
			} else if (weight < 0) {
				// Remove row
				const index = tableRows.findIndex((r) => r.orderId === order.id && r.userId === user.id);
				if (index >= 0) tableRows.splice(index, 1);
			}
		}
	}

	function removeOrder(orderId, userId) {
		// Find the order to remove
		const orderToRemove = { id: orderId, userId, amount: 0 };

		const delta = circuit.processIncrement(
			new ZSet([[orderToRemove, -1]]), // Negative weight = deletion
			new ZSet([])
		);

		// Apply deletion
		for (const [[order, user], weight] of delta.data) {
			if (weight < 0) {
				const index = tableRows.findIndex((r) => r.orderId === order.id && r.userId === user.id);
				if (index >= 0) tableRows.splice(index, 1);
			}
		}
	}

	function editBob() {
		// Step 1: Remove old Bob order (id: 2, amount: 200)
		const oldBobOrder = { id: 2, userId: 'bob', amount: 200 };

		// Step 2: Add new Bob order (id: 2, amount: 300)
		const newBobOrder = { id: 2, userId: 'bob', amount: Math.round(Math.random() * 1000) };

		// Create delta: remove old + add new
		// NOTE: there's no such thing as editing, we just remove the old and add the new
		const delta = circuit.processIncrement(
			new ZSet([
				[oldBobOrder, -1], // Remove old
				[newBobOrder, 1] // Add new
			]),
			new ZSet([]) // No user changes
		);

		// Apply delta to table
		for (const [[order, user], weight] of delta.data) {
			if (weight < 0) {
				// Remove old row
				const index = tableRows.findIndex((r) => r.orderId === order.id && r.userId === user.id);
				if (index >= 0) tableRows.splice(index, 1);
			} else if (weight > 0) {
				// Add new row
				tableRows.push({
					orderId: order.id,
					userId: user.id,
					userName: user.name,
					amount: order.amount
				});
			}
		}
	}

	// Example: Simulate adding new orders
	function handleAddOrder() {
		const newOrder = {
			id: Date.now(),
			userId: 'alice',
			amount: Math.floor(Math.random() * 1000)
		};
		addNewOrder(newOrder);
	}
</script>

<h1>this example shows surgical updates to the view using IVM and svelte's reactivity</h1>
<h2>
	make sure to turn paint flashing, layout shift, etc... in chrome `Rendering` option when you right
	click on console header
</h2>
<div>
	<button onclick={handleAddOrder}>Add New Order</button>
	<button onclick={editBob}> edit Bob amount via lib</button>

	<table>
		<thead>
			<tr>
				<th>Order ID</th>
				<th>User</th>
				<th>Amount</th>
				<th>Actions</th>
			</tr>
		</thead>
		<tbody>
			{#each sorted as row (row.orderId)}
				<tr>
					<td id={row.orderId}>{row.orderId}</td>
					<td>{row.userName} ({row.userId})</td>
					<td>${row.amount}</td>
					<td>
						<button onclick={() => removeOrder(row.orderId, row.userId)}> Remove </button>
					</td>
				</tr>
			{/each}
		</tbody>
	</table>

	<p>Total rows: {tableRows.length}</p>
</div>

<style>
	table {
		border-collapse: collapse;
		width: 100%;
	}

	th,
	td {
		border: 1px solid #ddd;
		padding: 8px;
		text-align: left;
	}

	th {
		background-color: #f4f4f4;
	}
</style>

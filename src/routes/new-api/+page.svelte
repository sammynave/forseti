<script lang="ts">
	import { createReactiveTable, createQuery } from '$lib/index.js';
	import { onDestroy } from 'svelte';

	// Types
	type Order = { id: string; userId: string; amount: number };
	type User = { id: string; name: string; age: number };
	type JoinedRow = { orderId: string; userId: string; userName: string; amount: number };

	const aliceId = crypto.randomUUID();
	const bobId = crypto.randomUUID();

	// Initialize with base data
	const initialOrders = [
		{ id: aliceId, userId: 'alice', amount: 100 },
		{ id: bobId, userId: 'bob', amount: 200 }
	];

	const initialUsers = [
		{ id: 'alice', name: 'Alice', age: 25 },
		{ id: 'bob', name: 'Bob', age: 30 }
	];

	// Create reactive tables - no manual circuit management needed
	const orders = createReactiveTable(initialOrders, 'id');
	const users = createReactiveTable(initialUsers, 'id');

	// Create reactive query with fluent API - all complexity hidden
	const sortedRowsQuery = createQuery(orders)
		.join(
			users,
			(order: Order) => order.userId,
			(user: User) => user.id
		)
		.select(
			(order: Order, user: User): JoinedRow => ({
				orderId: order.id,
				userId: user.id,
				userName: user.name,
				amount: order.amount
			})
		)
		.sortBy('orderId')
		.reactive();

	// Reactive state that automatically updates with any changes
	let sortedRows = $state<JoinedRow[]>([]);

	const unsub = sortedRowsQuery.subscribe((x) => {
		sortedRows = x;
	});

	// Simple CRUD operations - no manual delta processing needed
	function handleAddOrder() {
		const newOrder = {
			id: crypto.randomUUID(),
			userId: 'alice',
			amount: Math.floor(Math.random() * 1000)
		};
		orders.add(newOrder); // Automatically triggers incremental updates
	}

	function editBob() {
		orders.update(bobId, {
			amount: Math.round(Math.random() * 1000)
		}); // Automatically handles old/new record delta
	}

	function removeOrder(orderId: string) {
		orders.remove(orderId); // Automatically triggers incremental updates
	}

	onDestroy(() => {
		unsub();
	});
</script>

<h1>this example shows surgical updates to the view using IVM and svelte's reactivity</h1>
<h2>
	make sure to turn paint flashing, layout shift, etc... in chrome `Rendering` option when you right
	click on console header
</h2>
<div>
	<p>Total rows: {sortedRows.length}</p>
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
			{#each sortedRows as row (row.orderId)}
				<tr>
					<td id={row.orderId}>{row.orderId}</td>
					<td>{row.userName} ({row.userId})</td>
					<td>${row.amount}</td>
					<td>
						<button onclick={() => removeOrder(row.orderId)}> Remove </button>
					</td>
				</tr>
			{/each}
		</tbody>
	</table>
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

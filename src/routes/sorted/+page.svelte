<script>
	import { createStatefulJoinCircuit } from '$lib/stateful-circuit.js';
	import { ZSet, ZSetGroup } from '$lib/z-set.js';
	import { StatefulTopK } from '$lib/stateful-top-k.js';

	// Types
	// type Order = { id: number; userId: string; amount: number };
	// type User = { id: string; name: string; age: number };

	const aliceId = crypto.randomUUID();
	const bobId = crypto.randomUUID();

	// Create stateful circuit once (outside reactive context)
	const circuit = createStatefulJoinCircuit(
		(order) => order.userId,
		(user) => user.id
	);

	const sortCircuit = new StatefulTopK(
		(a, b) => a.orderId.localeCompare(b.orderId),
		Infinity, // No limit - sort all rows
		0, // No offset
		new ZSetGroup()
	);

	// Helper: Convert join delta to row delta
	function joinDeltaToRowDelta(joinDelta) {
		const rowDelta = new ZSet([]);
		for (const [[order, user], weight] of joinDelta.data) {
			const row = {
				orderId: order.id,
				userId: user.id,
				userName: user.name,
				amount: order.amount
			};
			rowDelta.append([row, weight]);
		}
		return rowDelta;
	}

	// Initialize with base data
	const initialOrders = [
		{ id: aliceId, userId: 'alice', amount: 100 },
		{ id: bobId, userId: 'bob', amount: 200 }
	];

	const initialUsers = [
		{ id: 'alice', name: 'Alice', age: 25 },
		{ id: 'bob', name: 'Bob', age: 30 }
	];

	circuit.initialize(
		new ZSet(initialOrders.map((o) => [o, 1])),
		new ZSet(initialUsers.map((u) => [u, 1]))
	);

	const initialJoinResult = circuit.getMaterializedView();
	const initialRowDelta = joinDeltaToRowDelta(initialJoinResult);
	sortCircuit.processIncrement(initialRowDelta); // Process initial data once
	let sortedRows = $state(sortCircuit.getCurrentState().allElements.map(([row, weight]) => row));

	// Function to process new data with surgical updates
	function addNewOrder(order) {
		const joinDelta = circuit.processIncrement(new ZSet([[order, 1]]), new ZSet([]));
		const rowDelta = joinDeltaToRowDelta(joinDelta);
		sortCircuit.processIncrement(rowDelta);
		sortedRows = sortCircuit.getCurrentState().allElements.map(([row, weight]) => row);
	}

	function removeOrder(order) {
		const joinDelta = circuit.processIncrement(new ZSet([[order, -1]]), new ZSet([]));
		const rowDelta = joinDeltaToRowDelta(joinDelta);
		sortCircuit.processIncrement(rowDelta);
		sortedRows = sortCircuit.getCurrentState().allElements.map(([row, weight]) => row);
	}

	function editBob() {
		const oldBob = sortedRows.find((x) => x.orderId === bobId);
		const oldBobOrder = { id: bobId, userId: 'bob', amount: oldBob.amount };
		const newBobOrder = { id: bobId, userId: 'bob', amount: Math.round(Math.random() * 1000) };
		const joinDelta = circuit.processIncrement(
			new ZSet([
				[oldBobOrder, -1],
				[newBobOrder, 1]
			]),
			new ZSet([])
		);

		const rowDelta = joinDeltaToRowDelta(joinDelta);
		sortCircuit.processIncrement(rowDelta);
		sortedRows = sortCircuit.getCurrentState().allElements.map(([row, weight]) => row);
	}

	// Example: Simulate adding new orders
	function handleAddOrder() {
		const newOrder = {
			id: crypto.randomUUID(),
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
			{#each sortedRows as row (row.orderId)}
				<tr>
					<td id={row.orderId}>{row.orderId}</td>
					<td>{row.userName} ({row.userId})</td>
					<td>${row.amount}</td>
					<td>
						<button
							onclick={() =>
								removeOrder({ id: row.orderId, userId: row.userId, amount: row.amount })}
						>
							Remove
						</button>
					</td>
				</tr>
			{/each}
		</tbody>
	</table>

	<p>Total rows: {sortedRows.length}</p>
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

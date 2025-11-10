<script lang="ts">
	import { onDestroy } from 'svelte';
	import { createQuery, createReactiveTable, ZSet, type BatchOperation } from '../../lib/index.js';

	interface User {
		id: string;
		name: string;
		email: string;
		status: 'active' | 'inactive';
	}

	// Initialize reactive table with some sample data
	let userTable = createReactiveTable<User>(
		[
			{ id: '1', name: 'Alice', email: 'alice@example.com', status: 'active' },
			{ id: '2', name: 'Bob', email: 'bob@example.com', status: 'active' }
		],
		'id'
	);

	// Track pending operations to prevent race conditions
	let pendingOperations: Record<string, boolean> = {};

	interface LogEntry {
		id: string;
		timestamp: string;
		message: string;
	}

	let users: User[] = $state([]);
	let top10users: User[] = $state([]);
	let logs: LogEntry[] = $state([]);

	function log(message: string) {
		const logEntry: LogEntry = {
			id: crypto.randomUUID(),
			timestamp: new Date().toLocaleTimeString(),
			message
		};
		logs = [logEntry, ...logs];
	}

	const top10Query = createQuery(userTable).sortBy('id').limit(10).reactive();
	top10Query.subscribe((x) => {
		top10users = x;
	});

	// Subscribe to table changes to update UI
	const unsubscribe = userTable.subscribe((delta: ZSet<User>) => {
		users = userTable.toArray();
		log(`Delta received: ${delta.data.length} changes`);
	});
	users = userTable.toArray();

	const userCountQuery = createQuery(userTable).count();
	// Reactive state that automatically updates with any changes
	let userCount = $state(0);

	const unsub = userCountQuery.subscribe((x) => {
		userCount = x;
	});

	onDestroy(() => {
		unsubscribe();
		unsub();
	});

	// Simulate the optimistic update pattern for SQLite persistence
	function simulateOptimisticUpsert() {
		const id = crypto.randomUUID();
		const operationId = `upsert_${id}`;

		// Prevent rapid clicking race conditions
		if (pendingOperations[operationId]) {
			log('⏳ Operation already in progress, ignoring');
			return;
		}

		pendingOperations[operationId] = true;

		const user: User = {
			id,
			name: 'New User',
			email: `user-${id.slice(-8)}@example.com`,
			status: 'active'
		};

		log(`🚀 Optimistic upsert started (${id.slice(-8)})`);

		// 1. Immediate optimistic update
		userTable.upsert(user);
		log('✅ Optimistic update applied immediately');

		// 2. Simulate SQLite worker processing (with delay)
		setTimeout(() => {
			// 3. Worker confirms with the same operation - idempotent!
			userTable.upsert(user);
			log(`🔄 SQLite worker confirmed (${id.slice(-8)})`);
			delete pendingOperations[operationId];
		}, 8000);
	}

	function simulateOptimisticDelete() {
		if (userCount === 0) return;

		const userId = users[0].id;
		log('🗑️ Optimistic delete started');

		// 1. Immediate optimistic removal
		const wasRemoved = userTable.safeRemove(userId);
		log(`✅ Optimistic removal: ${wasRemoved ? 'success' : 'already removed'}`);

		// 2. Simulate SQLite worker processing
		setTimeout(() => {
			// 3. Worker confirms deletion - idempotent!
			const confirmed = userTable.safeRemove(userId);
			log(`🔄 SQLite worker confirmed: ${confirmed ? 'deleted' : 'already removed'}`);
		}, 8000);
	}

	function simulateConflictResolution() {
		if (userCount === 0) return;

		const originalUser = users[0];
		log('⚠️ Simulating conflict resolution');

		// Simulate an optimistic update that was wrong
		const optimisticUser = { ...originalUser, name: 'Wrong Name', email: 'wrong@example.com' };
		userTable.upsert(optimisticUser);
		log('❌ Applied incorrect optimistic update');

		setTimeout(() => {
			// Worker discovered conflict and sends correction
			const correctUser = { ...originalUser, name: 'Correct Name', email: 'correct@example.com' };
			const correctionDelta = new ZSet<User>([
				[optimisticUser, -1], // Remove incorrect
				[correctUser, 1] // Add correct
			]);

			userTable.applyDelta(correctionDelta);
			log('✅ Conflict resolved with applyDelta');
		}, 8000);
	}

	function simulateBatchOperations() {
		log('📦 Simulating batch operations from SQLite worker');

		const batch = Array.from({ length: 1000 }).map((x, i) => {
			const num = i + 1000 + userCount;
			return {
				type: 'upsert',
				item: {
					id: `${num}`,
					name: `Batch User ${num}`,
					email: `batch${num}@example.com`,
					status: 'active'
				}
			};
		});

		userTable.batch(batch);
		log('✅ Batch operations completed - single notification sent');
	}

	function simulateInitialDataLoad() {
		log('📥 Simulating initial data load from SQLite');

		const sqliteData: User[] = [
			{ id: '500', name: 'Loaded User 1', email: 'loaded1@example.com', status: 'active' },
			{ id: '501', name: 'Loaded User 2', email: 'loaded2@example.com', status: 'active' },
			{ id: '502', name: 'Loaded User 3', email: 'loaded3@example.com', status: 'inactive' }
		];

		// Convert to ZSet and apply via applyDelta (efficient for large datasets)
		const initialZSet = new ZSet(sqliteData.map((user) => [user, 1] as [User, number]));
		userTable.applyDelta(initialZSet);
		log('✅ Initial data loaded via applyDelta');
	}

	function clearAll() {
		// Create new table (simulates fresh start)
		userTable.clear();
		logs = [];
		pendingOperations = {};
		log('🧹 Cleared all data');
	}
</script>

<h1>SQLite + Optimistic Updates Demo</h1>
<p>
	This demo shows how to use forseti's idempotent operations for optimistic updates with SQLite
	persistence. The pattern: apply changes immediately for 120fps UI, then confirm with SQLite
	worker.
</p>

<div class="controls">
	<button onclick={simulateOptimisticUpsert}>🚀 Optimistic Upsert</button>
	<button onclick={simulateOptimisticDelete}>🗑️ Optimistic Delete</button>
	<button onclick={simulateConflictResolution}>⚠️ Conflict Resolution</button>
	<button onclick={simulateBatchOperations}>📦 Batch Operations</button>
	<button onclick={simulateInitialDataLoad}>📥 Initial Data Load</button>
	<button onclick={clearAll}>🧹 Clear All</button>
</div>

<div class="layout">
	<div class="users">
		<h2>Users ({users.length})({userCount}) limited to 10 in this table</h2>
		<div class="user-grid">
			{#each top10users as user (user.id)}
				<div class="user-card" class:inactive={user.status === 'inactive'}>
					<div><strong>#{user.id}</strong></div>
					<div>{user.name}</div>
					<div class="email">{user.email}</div>
					<div class="status">{user.status}</div>
				</div>
			{:else}
				<div class="empty">No users yet...</div>
			{/each}
			<h3>TODO implement paging</h3>
		</div>
	</div>

	<div class="logs">
		<h2>Operation Log</h2>
		<div class="log-container">
			{#each logs as logEntry (logEntry.id)}
				<div class="log-entry">{logEntry.timestamp}: {logEntry.message}</div>
			{/each}
		</div>
	</div>
</div>

<div class="explanation">
	<h2>What's Happening</h2>
	<ul>
		<li>
			<strong>🚀 Optimistic Upsert:</strong> Immediately updates UI, then SQLite worker confirms with
			same operation (idempotent)
		</li>
		<li>
			<strong>🗑️ Optimistic Delete:</strong> Immediately removes from UI, worker confirmation is safe
			even if already removed
		</li>
		<li>
			<strong>⚠️ Conflict Resolution:</strong> Shows how to correct wrong optimistic updates using applyDelta
		</li>
		<li>
			<strong>📦 Batch Operations:</strong> Efficient bulk operations with single notification to subscribers
		</li>
		<li>
			<strong>📥 Initial Data Load:</strong> Fast loading of large datasets from SQLite using applyDelta
		</li>
	</ul>

	<h3>Code Pattern</h3>
	<pre><code
			>{`// 1. Optimistic update (immediate)
userTable.upsert(user);

// 2. Send to SQLite worker (your code)
worker.postMessage({ type: 'upsert', data: user });

// 3. Worker confirms - idempotent, no errors!
worker.onmessage = (e) => {
  if (e.data.type === 'upsert_confirmed') {
    userTable.upsert(e.data.item); // Safe to call again
  }
};`}</code
		></pre>
</div>

<style>
	.controls {
		display: flex;
		gap: 10px;
		margin: 20px 0;
		flex-wrap: wrap;
	}

	button {
		padding: 8px 16px;
		background: #007bff;
		color: white;
		border: none;
		border-radius: 4px;
		cursor: pointer;
		font-size: 14px;
	}

	button:hover {
		background: #0056b3;
	}

	.layout {
		display: grid;
		grid-template-columns: 1fr 1fr;
		gap: 20px;
		margin-top: 20px;
	}

	.user-grid {
		display: grid;
		gap: 10px;
	}

	.user-card {
		border: 2px solid #28a745;
		border-radius: 8px;
		padding: 12px;
		background: #f8f9fa;
	}

	.user-card.inactive {
		border-color: #6c757d;
		opacity: 0.7;
	}

	.user-card strong {
		color: #007bff;
	}

	.email {
		color: #6c757d;
		font-size: 12px;
		font-family: monospace;
	}

	.status {
		font-size: 12px;
		font-weight: bold;
		text-transform: uppercase;
		margin-top: 4px;
	}

	.empty {
		color: #6c757d;
		font-style: italic;
		padding: 20px;
		text-align: center;
	}

	.logs h2 {
		margin-top: 0;
	}

	.log-container {
		max-height: 500px;
		overflow-y: auto;
		border: 1px solid #dee2e6;
		border-radius: 4px;
		background: #f8f9fa;
	}

	.log-entry {
		padding: 4px 8px;
		border-bottom: 1px solid #dee2e6;
		font-family: monospace;
		font-size: 12px;
	}

	.log-entry:last-child {
		border-bottom: none;
	}

	.explanation {
		margin-top: 40px;
		padding: 20px;
		background: #e9ecef;
		border-radius: 8px;
	}

	.explanation ul {
		margin: 16px 0;
	}

	.explanation li {
		margin: 8px 0;
	}

	pre {
		background: #f8f9fa;
		border: 1px solid #dee2e6;
		border-radius: 4px;
		padding: 16px;
		overflow-x: auto;
		margin-top: 16px;
	}

	code {
		font-family: 'Monaco', 'Consolas', monospace;
		font-size: 14px;
	}

	@media (max-width: 768px) {
		.layout {
			grid-template-columns: 1fr;
		}

		.controls {
			justify-content: center;
		}

		button {
			font-size: 12px;
			padding: 6px 12px;
		}
	}
</style>

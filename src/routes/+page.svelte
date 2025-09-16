<script lang="ts">
	import {
		todoDeletedEvent,
		EventStore,
		todoCreatedEvent,
		todoToggledEvent
	} from '$lib/event-store.js';
	import { ZSet } from '$lib/stream.js';
	import { onMount } from 'svelte';

	type Todo = { id: string; title: string; done: boolean };
	let newTitle = $state('');

	onMount(() => {
		window.debug = true;
	});
	const eventStore = new EventStore();

	let events = $state(JSON.stringify(eventStore.events, null, 2));
	let debug = $state(JSON.stringify(Object.fromEntries(eventStore.getZSetDebug()), null, 2));

	let currentSnapshot = $state(new ZSet());

	// Subscribe to EventStore changes
	$effect(() => {
		const unsubscribe = eventStore.subscribe((snapshot) => {
			currentSnapshot = snapshot;
		});

		return unsubscribe; // Cleanup
	});

	let todos = $derived<Todo[]>(currentSnapshot.materialize);
	let alpha = $derived.by(() => todos.toSorted((a, b) => a.title.localeCompare(b.title)));

	function submit(e: SubmitEvent) {
		e.preventDefault();
		const formData = new FormData(e.target as HTMLFormElement);
		const title = formData.get('title');

		if (
			(title && typeof title === 'string' && title.length === 0) ||
			!title ||
			typeof title !== 'string'
		) {
			throw Error('No title');
		}

		// Z-set approach: just append event, Z-set handles incremental update
		eventStore.append(todoCreatedEvent({ title }));

		newTitle = '';
	}

	function toggleTodo(id: string) {
		// Z-set approach: append event, get updated todos
		eventStore.append(todoToggledEvent({ id }));
	}

	function deleteTodo(id: string) {
		// Z-set approach: append event, get updated todos
		eventStore.append(todoDeletedEvent({ id }));
	}
</script>

<div class="page">
	<div class="left">
		<form onsubmit={submit}>
			<label for="title">title</label>
			<br />
			<input type="text" name="title" id="title" bind:value={newTitle} />
			<button type="submit">save</button>
		</form>

		{#each alpha as todo (todo.id)}
			<div>
				<label for={todo.id}>
					<input
						type="checkbox"
						id={todo.id}
						checked={todo.done}
						oninput={() => {
							toggleTodo(todo.id);
						}}
					/>
					{todo.title}</label
				>
				<button onclick={() => deleteTodo(todo.id)}> x</button>
			</div>
		{/each}
	</div>
	<div class="right">
		<h3>Current Todos (Z-set projection)</h3>
		<pre>{JSON.stringify(todos, null, 2)}</pre>

		<h3>Event Log</h3>
		<pre>{events}</pre>

		<h3>Z-set Debug (Raw weights)</h3>
		<pre>{debug}</pre>
	</div>
</div>

<style>
	.page {
		display: grid;
		grid-template-columns: 1fr 1fr;
	}
	.right {
		font-size: 12px;
		display: flex;
		word-wrap: break-word;
	}
</style>

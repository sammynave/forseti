<script lang="ts">
	import {
		deleteToggledEvent,
		EventStore,
		todoCreatedEvent,
		todoToggledEvent
	} from '$lib/event-store.js';

	type Todo = { id: string; title: string; done: boolean };
	let todos = $state<Todo[]>([]);
	let alpha = $derived.by(() => todos.toSorted((a, b) => a.title.localeCompare(b.title)));
	let newTitle = $state('');

	const eventStore = new EventStore();
	let events = $state(JSON.stringify(eventStore.events, null, 2));
	let debug = $state(JSON.stringify(Object.fromEntries(eventStore.getZSetDebug()), null, 2));

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
		todos = eventStore.getTodos(); // O(n) projection, but Z-set update was O(1)
		events = JSON.stringify(eventStore.events, null, 2);
		debug = JSON.stringify(Object.fromEntries(eventStore.getZSetDebug()), null, 2);

		newTitle = '';
	}

	function toggleTodo(id: string) {
		// Z-set approach: append event, get updated todos
		eventStore.append(todoToggledEvent({ id }));
		todos = eventStore.getTodos();
		events = JSON.stringify(eventStore.events, null, 2);
		debug = JSON.stringify(Object.fromEntries(eventStore.getZSetDebug()), null, 2);
	}

	function deleteTodo(id: string) {
		// Z-set approach: append event, get updated todos
		eventStore.append(deleteToggledEvent({ id }));
		todos = eventStore.getTodos();
		events = JSON.stringify(eventStore.events, null, 2);
		debug = JSON.stringify(Object.fromEntries(eventStore.getZSetDebug()), null, 2);
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

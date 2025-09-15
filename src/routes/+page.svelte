<script lang="ts">
	import {
		EventStore,
		todoCreatedEvent,
		todoToggledEvent,
		rebuildTodosFromEvents
	} from '$lib/event-store.js';

	type Todo = { id: string; title: string; done: boolean };
	let todos = $state<Todo[]>([]);
	let newTitle = $state('');
	const eventStore = new EventStore();

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

		eventStore.append(todoCreatedEvent({ title }));
		todos = rebuildTodosFromEvents(eventStore.events);

		newTitle = '';
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

		{#each todos as todo (todo.id)}
			<div>
				<label for={todo.id}>
					<input
						type="checkbox"
						id={todo.id}
						checked={todo.done}
						oninput={() => {
							eventStore.append(todoToggledEvent({ id: todo.id }));
							todos = rebuildTodosFromEvents(eventStore.events);
						}}
					/>
					{todo.title}</label
				>
			</div>
		{/each}
	</div>
	<pre class="right">{JSON.stringify(todos, null, 2)}</pre>
</div>

<style>
	.page {
		display: grid;
		grid-template-columns: 1fr 1fr;
	}
</style>

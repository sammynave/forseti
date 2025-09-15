<script lang="ts">
	type Todo = { id: string; title: string; done: boolean };
	let todos = $state<Todo[]>([]);
	let newTitle = $state('');

	function submit(e: SubmitEvent) {
		e.preventDefault();
		const formData = new FormData(e.target);
		todos.push({ id: crypto.randomUUID(), title: formData.get('title'), done: false });
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
						oninput={(e: Event) => {
							todo.done = e.target.checked;
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

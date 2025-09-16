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

	const eventStore = new EventStore();
	let loading = $state(true);

	onMount(() => {
		window.debug = false;
		console.log('🚀 Starting 20,000 event stress test...');
		const startTime = performance.now();

		// Create some initial todos
		const todoIds: string[] = [];
		for (let i = 0; i < 1000; i++) {
			const event = todoCreatedEvent({ title: `Todo ${i}` });
			eventStore.append(event);
			todoIds.push(event.id);
		}

		// Mix of operations for remaining 9,000 events
		for (let i = 1000; i < 10000; i++) {
			const rand = Math.random();

			if (rand < 0.4 && todoIds.length > 0) {
				// 40% toggle existing todo
				const randomId = todoIds[Math.floor(Math.random() * todoIds.length)];
				eventStore.append(todoToggledEvent({ id: randomId }));
			} else if (rand < 0.5 && todoIds.length > 0) {
				// 10% delete existing todo
				const randomIndex = Math.floor(Math.random() * todoIds.length);
				const randomId = todoIds[randomIndex];
				eventStore.append(todoDeletedEvent({ id: randomId }));
				todoIds.splice(randomIndex, 1);
			} else {
				// 50% create new todo
				const event = todoCreatedEvent({ title: `Stress Todo ${i}` });
				eventStore.append(event);
				todoIds.push(event.id);
			}
		}

		const endTime = performance.now();
		console.log(`✅ 20,000 events processed in ${(endTime - startTime).toFixed(2)}ms`);
		loading = false;
		window.debug = true;
	});

	let currentSnapshot = $state(new ZSet());

	// Subscribe to EventStore changes
	$effect(() => {
		const unsubscribe = eventStore.subscribe((snapshot) => {
			currentSnapshot = snapshot;
		});

		return unsubscribe; // Cleanup
	});

	let todos = $derived<Todo[]>(currentSnapshot.materialize);

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
	{#if loading}<h1>loading</h1>{/if}
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
							toggleTodo(todo.id);
						}}
					/>
					{todo.title}</label
				>
				<button onclick={() => deleteTodo(todo.id)}> x</button>
			</div>
		{/each}
	</div>
</div>

<style>
	.page {
		display: grid;
		grid-template-columns: 1fr 1fr;
	}
</style>

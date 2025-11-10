<script lang="ts">
	import { dev } from '$app/environment';
	import { RenderScan } from 'svelte-render-scan';
	let { children } = $props();
</script>

{#if dev}
	<RenderScan />
{/if}
{@render children()}

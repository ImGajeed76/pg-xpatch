<script lang="ts">
	import './layout.css';
	import { ModeWatcher } from 'mode-watcher';

	let { children } = $props();
</script>

<ModeWatcher />
<div class="min-h-screen bg-background text-foreground">
	{@render children()}
</div>

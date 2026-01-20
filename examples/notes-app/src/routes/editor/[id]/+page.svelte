<script lang="ts">
	import { page } from '$app/state';
	import { goto } from '$app/navigation';
	import { onDestroy, onMount } from 'svelte';
	import {
		getNote,
		getVersions,
		createVersion,
		getStats,
		formatBytes,
		formatRelativeTime,
		type Note,
		type NoteVersion,
		type NoteStats
	} from '$lib/api';
	import { Button } from '$lib/components/ui/button/index.js';
	import { Textarea } from '$lib/components/ui/textarea/index.js';
	import * as Popover from '$lib/components/ui/popover/index.js';
	import { Slider } from '$lib/components/ui/slider/index.js';
	import {
		ArrowLeft,
		Eye,
		Edit,
		Clock,
		ChevronLeft,
		ChevronRight,
		ChevronsLeft,
		ChevronsRight
	} from '@lucide/svelte';
	import Markdown from 'svelte-exmarkdown';
	import rehypeHighlight from 'rehype-highlight';
	import type { Plugin } from 'svelte-exmarkdown';
	import 'highlight.js/styles/github-dark.css';

	const SAVE_THROTTLE_MS = 500;
	const plugins: Plugin[] = [{ rehypePlugin: rehypeHighlight }];

	let noteId = $derived(page.params.id!);
	let note = $state<Note | null>(null);
	let versions = $state<NoteVersion[]>([]);
	let content = $state('');
	let lastSavedContent = $state('');
	let mode = $state<'write' | 'preview'>('write');
	let stats = $state<NoteStats | null>(null);
	let isTimeTravel = $state(false);
	let timeTravelIndex = $state(0);
	let timeTravelContent = $state('');
	let isPopoverOpen = $state(false);
	let saveTimeout: ReturnType<typeof setTimeout> | null = null;
	let keyHoldInterval: ReturnType<typeof setInterval> | null = null;
	let keyHoldTimeout: ReturnType<typeof setTimeout> | null = null;
	let loading = $state(true);

	onMount(async () => {
		await loadData();
		loading = false;
	});

	onDestroy(() => {
		if (saveTimeout) clearTimeout(saveTimeout);
		if (keyHoldInterval) clearInterval(keyHoldInterval);
		if (keyHoldTimeout) clearTimeout(keyHoldTimeout);
	});

	async function loadData() {
		note = await getNote(noteId);
		versions = await getVersions(noteId);
		stats = await getStats(noteId);

		if (versions.length > 0) {
			content = versions[0].content;
			lastSavedContent = content;
			timeTravelIndex = versions.length - 1;
		}
	}

	async function performSave() {
		if (content === lastSavedContent) return;

		try {
			const newVersion = await createVersion(noteId, content);
			versions = [newVersion, ...versions];
			lastSavedContent = content;
			stats = await getStats(noteId);

			if (!isTimeTravel) {
				timeTravelIndex = versions.length - 1;
			}
		} catch (error) {
			console.error('Failed to save:', error);
		}
	}

	function scheduleContentSave() {
		// THROTTLE: Only schedule if no save is pending
		if (saveTimeout !== null) return;

		saveTimeout = setTimeout(async () => {
			await performSave();
			saveTimeout = null;
		}, SAVE_THROTTLE_MS);
	}

	async function handleTimeTravelChange(index: number) {
		if (index < 0 || index >= versions.length) return;

		timeTravelIndex = index;
		isTimeTravel = true;

		// versions are ordered DESC (newest first), so index 0 = newest, length-1 = oldest
		const version = versions[versions.length - 1 - index];
		timeTravelContent = version?.content ?? '';
	}

	async function stepVersion(delta: number) {
		const newIndex = timeTravelIndex + delta;
		if (newIndex >= 0 && newIndex < versions.length) {
			await handleTimeTravelChange(newIndex);
		}
	}

	function exitTimeTravel() {
		isTimeTravel = false;
		timeTravelContent = '';
		timeTravelIndex = versions.length - 1;
	}

	function restoreVersion() {
		const version = versions[versions.length - 1 - timeTravelIndex];
		if (version) {
			content = version.content;
			exitTimeTravel();
			scheduleContentSave();
		}
	}

	function handleKeyDown(e: KeyboardEvent) {
		if (!isPopoverOpen) return;

		if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
			e.preventDefault();

			if (keyHoldInterval) {
				clearInterval(keyHoldInterval);
				keyHoldInterval = null;
			}
			if (keyHoldTimeout) {
				clearTimeout(keyHoldTimeout);
				keyHoldTimeout = null;
			}

			const direction = e.key === 'ArrowRight' ? 1 : -1;

			// Immediate first step
			stepVersion(direction);

			// After delay, start rapid fire (20 steps per second)
			keyHoldTimeout = setTimeout(() => {
				keyHoldInterval = setInterval(() => {
					stepVersion(direction);
				}, 50);
			}, 300);
		}
	}

	function handleKeyUp(e: KeyboardEvent) {
		if (e.key === 'ArrowLeft' || e.key === 'ArrowRight') {
			if (keyHoldInterval) {
				clearInterval(keyHoldInterval);
				keyHoldInterval = null;
			}
			if (keyHoldTimeout) {
				clearTimeout(keyHoldTimeout);
				keyHoldTimeout = null;
			}
		}
	}

	// Auto-save effect
	$effect(() => {
		if (!isTimeTravel && content !== lastSavedContent) {
			scheduleContentSave();
		}
	});

	// Keyboard event listeners for time travel
	$effect(() => {
		if (isPopoverOpen) {
			window.addEventListener('keydown', handleKeyDown);
			window.addEventListener('keyup', handleKeyUp);
		} else {
			window.removeEventListener('keydown', handleKeyDown);
			window.removeEventListener('keyup', handleKeyUp);
			if (keyHoldInterval) {
				clearInterval(keyHoldInterval);
				keyHoldInterval = null;
			}
			if (keyHoldTimeout) {
				clearTimeout(keyHoldTimeout);
				keyHoldTimeout = null;
			}
		}

		return () => {
			window.removeEventListener('keydown', handleKeyDown);
			window.removeEventListener('keyup', handleKeyUp);
		};
	});

	// Computed: current version being viewed in time travel
	let currentTimeTravelVersion = $derived(
		isTimeTravel ? versions[versions.length - 1 - timeTravelIndex] : null
	);
</script>

<svelte:head>
	<title>{note?.title ?? 'Loading...'} - Notes</title>
</svelte:head>

{#if loading}
	<div class="h-screen flex items-center justify-center">
		<p class="text-muted-foreground">Loading...</p>
	</div>
{:else}
	<div class="h-screen flex flex-col">
		<!-- Header -->
		<header class="border-b p-4 flex items-center justify-between">
			<div class="flex items-center gap-4">
				<Button variant="ghost" size="icon" aria-label="Back to notes" onclick={() => goto('/')}>
					<ArrowLeft class="h-4 w-4" />
				</Button>

				{#if stats && stats.totalRows > 0}
					<div class="text-sm text-muted-foreground hidden md:block">
						<span class="font-mono font-bold text-green-600 dark:text-green-500">
							{formatBytes(stats.compressedSizeBytes)}
						</span>
						<span class="mx-2">/</span>
						<span class="font-mono">
							{formatBytes(stats.rawSizeBytes)}
						</span>
						<span class="ml-2">
							({stats.compressionRatio.toFixed(1)}x compression)
						</span>
						<span class="ml-2 text-xs">
							{stats.totalRows}
							{stats.totalRows === 1 ? 'version' : 'versions'}
						</span>
						{#if stats.totalRows > 1}
							<span class="ml-2 text-xs font-bold text-primary">
								avg {formatBytes(Math.round(stats.compressedSizeBytes / stats.totalRows))}/version
							</span>
						{/if}
					</div>
				{/if}
			</div>

			<div class="flex items-center gap-2">
				<!-- Mode toggle -->
				<Button
					variant={mode === 'write' ? 'default' : 'outline'}
					size="sm"
					onclick={() => (mode = 'write')}
				>
					<Edit class="h-4 w-4 mr-2" />
					Write
				</Button>
				<Button
					variant={mode === 'preview' ? 'default' : 'outline'}
					size="sm"
					onclick={() => (mode = 'preview')}
				>
					<Eye class="h-4 w-4 mr-2" />
					Preview
				</Button>

				<!-- Time Travel -->
				{#if versions.length > 0}
					<Popover.Root bind:open={isPopoverOpen}>
						<Popover.Trigger
							class="inline-flex items-center justify-center gap-2 whitespace-nowrap rounded-md text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring disabled:pointer-events-none disabled:opacity-50 border border-input bg-background shadow-sm hover:bg-accent hover:text-accent-foreground h-8 px-3"
						>
							<Clock class="h-4 w-4" />
							Time Travel ({versions.length})
						</Popover.Trigger>
						<Popover.Content class="w-[420px] mr-4">
							<div class="space-y-4">
								<div class="flex items-center justify-between">
									<h4 class="font-medium">Version History</h4>
									<span class="text-xs text-muted-foreground">
										{timeTravelIndex + 1} / {versions.length}
									</span>
								</div>

								<div class="text-sm">
									<div class="font-medium mb-1">
										{#if isTimeTravel && currentTimeTravelVersion}
											{new Date(currentTimeTravelVersion.createdAt).toLocaleString()}
										{:else}
											Current version
										{/if}
									</div>
									{#if isTimeTravel && currentTimeTravelVersion}
										<div class="text-xs text-muted-foreground">
											{formatRelativeTime(currentTimeTravelVersion.createdAt)}
										</div>
									{/if}
								</div>

								<!-- Slider for quick navigation -->
								<div>
									<span class="text-xs text-muted-foreground mb-2 block">
										Quick navigation
									</span>
									<Slider
										type="single"
										value={timeTravelIndex}
										max={versions.length - 1}
										step={1}
										onValueChange={(v: number) => handleTimeTravelChange(v)}
									/>
								</div>

								<!-- Fine-grained controls -->
								<div>
									<span class="text-xs text-muted-foreground mb-2 block">
										Precise control - Use arrow keys
									</span>
									<div class="flex items-center gap-2">
										<Button
											variant="outline"
											size="sm"
											aria-label="Go to first version"
											onclick={() => handleTimeTravelChange(0)}
											disabled={timeTravelIndex === 0}
											class="flex-1"
										>
											<ChevronsLeft class="h-3 w-3" />
										</Button>

										<Button
											variant="outline"
											size="sm"
											onclick={() => stepVersion(-1)}
											disabled={timeTravelIndex === 0}
											class="flex-1"
										>
											<ChevronLeft class="h-3 w-3 mr-1" />
											Prev
										</Button>

										<Button
											variant="outline"
											size="sm"
											onclick={() => stepVersion(1)}
											disabled={timeTravelIndex === versions.length - 1}
											class="flex-1"
										>
											Next
											<ChevronRight class="h-3 w-3 ml-1" />
										</Button>

										<Button
											variant="outline"
											size="sm"
											aria-label="Go to latest version"
											onclick={() => handleTimeTravelChange(versions.length - 1)}
											disabled={timeTravelIndex === versions.length - 1}
											class="flex-1"
										>
											<ChevronsRight class="h-3 w-3" />
										</Button>
									</div>
								</div>

								{#if isTimeTravel}
									<div class="flex gap-2">
										<Button size="sm" onclick={restoreVersion} class="flex-1">
											Restore This Version
										</Button>
										<Button variant="outline" size="sm" onclick={exitTimeTravel} class="flex-1">
											Return to Current
										</Button>
									</div>
								{/if}
							</div>
						</Popover.Content>
					</Popover.Root>
				{/if}
			</div>
		</header>

		<!-- Main content -->
		<main class="flex-1 overflow-auto p-6">
			{#if isTimeTravel}
				<div class="max-w-4xl mx-auto">
					<div class="mb-4 p-4 bg-accent rounded border">
						<p class="text-sm font-medium">
							Viewing: {currentTimeTravelVersion
								? new Date(currentTimeTravelVersion.createdAt).toLocaleString()
								: 'Unknown'}
						</p>
					</div>
					<div class="prose dark:prose-invert max-w-none">
						<Markdown md={timeTravelContent} {plugins} />
					</div>
				</div>
			{:else if mode === 'write'}
				<Textarea
					bind:value={content}
					placeholder="Start writing in Markdown..."
					class="min-h-[calc(100vh-200px)] font-mono resize-none max-w-4xl mx-auto"
				/>
			{:else}
				<div class="max-w-4xl mx-auto prose dark:prose-invert">
					<Markdown md={content} {plugins} />
				</div>
			{/if}
		</main>
	</div>
{/if}

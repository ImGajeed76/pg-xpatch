<script lang="ts">
	import { goto } from '$app/navigation';
	import { onMount } from 'svelte';
	import { getNotes, createNote, deleteNote, type Note } from '$lib/api';
	import { Button } from '$lib/components/ui/button/index.js';
	import { Input } from '$lib/components/ui/input/index.js';
	import * as Card from '$lib/components/ui/card/index.js';
	import { Trash2, FileText } from '@lucide/svelte';

	let notes = $state<Note[]>([]);
	let newNoteTitle = $state('');
	let loading = $state(true);

	onMount(async () => {
		notes = await getNotes();
		loading = false;
	});

	async function handleCreate() {
		if (!newNoteTitle.trim()) return;
		const note = await createNote(newNoteTitle);
		newNoteTitle = '';
		goto(`/editor/${note.id}`);
	}

	async function handleDelete(id: string, e: Event) {
		e.preventDefault();
		e.stopPropagation();
		await deleteNote(id);
		notes = notes.filter((n) => n.id !== id);
	}

	function handleKeydown(e: KeyboardEvent) {
		if (e.key === 'Enter') {
			handleCreate();
		}
	}

	function handleCardKeydown(e: KeyboardEvent, noteId: string) {
		if (e.key === 'Enter' || e.key === ' ') {
			e.preventDefault();
			goto(`/editor/${noteId}`);
		}
	}
</script>

<svelte:head>
	<title>Notes - pg-xpatch Demo</title>
</svelte:head>

<div class="container mx-auto p-8 max-w-4xl">
	<h1 class="text-3xl font-semibold tracking-tight leading-tight mb-8">pg-xpatch Notes</h1>

	<Card.Root class="mb-8">
		<Card.Header>
			<Card.Title>Create Note</Card.Title>
		</Card.Header>
		<Card.Content>
			<div class="flex gap-2">
				<Input
					bind:value={newNoteTitle}
					placeholder="Note title..."
					onkeydown={handleKeydown}
				/>
				<Button onclick={handleCreate} disabled={!newNoteTitle.trim()}>Create</Button>
			</div>
		</Card.Content>
	</Card.Root>

	{#if loading}
		<p class="text-muted-foreground">Loading...</p>
	{:else if notes.length === 0}
		<div class="flex flex-col items-center justify-center py-12 text-center">
			<FileText class="text-muted-foreground mb-4 h-12 w-12" />
			<h3 class="mb-2 text-lg font-medium">No notes yet</h3>
			<p class="text-muted-foreground mb-4 text-sm">
				Create your first note to get started
			</p>
		</div>
	{:else}
		<div class="grid gap-4">
			{#each notes as note (note.id)}
				<Card.Root
					class="cursor-pointer hover:bg-accent transition-colors duration-150"
					role="button"
					tabindex={0}
					onclick={() => goto(`/editor/${note.id}`)}
					onkeydown={(e) => handleCardKeydown(e, note.id)}
				>
					<Card.Header class="flex flex-row items-center justify-between">
						<div>
							<Card.Title>{note.title}</Card.Title>
							<Card.Description>
								{new Date(note.createdAt).toLocaleString()}
							</Card.Description>
						</div>
						<Button
							variant="ghost"
							size="icon"
							class="text-destructive hover:text-destructive"
							aria-label="Delete note"
							onclick={(e) => handleDelete(note.id, e)}
						>
							<Trash2 class="h-4 w-4" />
						</Button>
					</Card.Header>
				</Card.Root>
			{/each}
		</div>
	{/if}
</div>

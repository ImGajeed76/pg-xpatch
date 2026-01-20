export interface Note {
	id: string;
	title: string;
	createdAt: Date;
}

export interface NoteVersion {
	id: string;
	noteId: string;
	content: string;
	versionNum: number;
	createdAt: Date;
}

export interface NoteStats {
	totalRows: number;
	totalGroups: number;
	keyframeCount: number;
	deltaCount: number;
	rawSizeBytes: number;
	compressedSizeBytes: number;
	compressionRatio: number;
	cacheHits: number;
	cacheMisses: number;
	avgChainLength: number;
}

export async function getNotes(): Promise<Note[]> {
	const res = await fetch('/api/notes');
	if (!res.ok) throw new Error('Failed to fetch notes');
	return res.json();
}

export async function createNote(title: string): Promise<Note> {
	const res = await fetch('/api/notes', {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ title })
	});
	if (!res.ok) throw new Error('Failed to create note');
	return res.json();
}

export async function deleteNote(id: string): Promise<void> {
	const res = await fetch(`/api/notes/${id}`, { method: 'DELETE' });
	if (!res.ok) throw new Error('Failed to delete note');
}

export async function getNote(id: string): Promise<Note> {
	const res = await fetch(`/api/notes/${id}`);
	if (!res.ok) throw new Error('Failed to fetch note');
	return res.json();
}

export async function getVersions(noteId: string): Promise<NoteVersion[]> {
	const res = await fetch(`/api/notes/${noteId}/versions`);
	if (!res.ok) throw new Error('Failed to fetch versions');
	return res.json();
}

export async function createVersion(noteId: string, content: string): Promise<NoteVersion> {
	const res = await fetch(`/api/notes/${noteId}/versions`, {
		method: 'POST',
		headers: { 'Content-Type': 'application/json' },
		body: JSON.stringify({ content })
	});
	if (!res.ok) throw new Error('Failed to create version');
	return res.json();
}

export async function getStats(noteId: string): Promise<NoteStats | null> {
	const res = await fetch(`/api/notes/${noteId}/stats`);
	if (!res.ok) return null;
	return res.json();
}

export function formatBytes(bytes: number): string {
	if (bytes === 0) return '0 B';
	const k = 1024;
	const sizes = ['B', 'KB', 'MB'];
	const i = Math.floor(Math.log(bytes) / Math.log(k));
	return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

export function formatRelativeTime(date: Date): string {
	const now = Date.now();
	const timestamp = new Date(date).getTime();
	const diff = now - timestamp;
	const minutes = Math.floor(diff / 60000);
	const hours = Math.floor(diff / 3600000);
	const days = Math.floor(diff / 86400000);

	if (minutes < 1) return 'just now';
	if (minutes < 60) return `${minutes}m ago`;
	if (hours < 24) return `${hours}h ago`;
	if (days < 7) return `${days}d ago`;
	return new Date(timestamp).toLocaleDateString();
}

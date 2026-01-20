import { json } from '@sveltejs/kit';
import { db, notes, noteVersions } from '$lib/server/db';
import { desc } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	const allNotes = await db
		.select({
			id: notes.id,
			title: notes.title,
			createdAt: notes.createdAt
		})
		.from(notes)
		.orderBy(desc(notes.createdAt));

	return json(allNotes);
};

export const POST: RequestHandler = async ({ request }) => {
	const { title } = await request.json();

	if (!title?.trim()) {
		return json({ error: 'Title is required' }, { status: 400 });
	}

	const [newNote] = await db.insert(notes).values({ title: title.trim() }).returning();

	await db.insert(noteVersions).values({
		noteId: newNote.id,
		content: '',
		versionNum: 1
	});

	return json(newNote);
};

import { json, error } from '@sveltejs/kit';
import { db, notes } from '$lib/server/db';
import { eq } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ params }) => {
	const [note] = await db.select().from(notes).where(eq(notes.id, params.id)).limit(1);

	if (!note) {
		error(404, 'Note not found');
	}

	return json(note);
};

export const DELETE: RequestHandler = async ({ params }) => {
	const [deleted] = await db.delete(notes).where(eq(notes.id, params.id)).returning();

	if (!deleted) {
		error(404, 'Note not found');
	}

	return json({ success: true });
};

import { json } from '@sveltejs/kit';
import { db, noteVersions } from '$lib/server/db';
import { eq, desc } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async ({ params }) => {
	const versions = await db
		.select({
			id: noteVersions.id,
			noteId: noteVersions.noteId,
			content: noteVersions.content,
			versionNum: noteVersions.versionNum,
			createdAt: noteVersions.createdAt
		})
		.from(noteVersions)
		.where(eq(noteVersions.noteId, params.id))
		.orderBy(desc(noteVersions.versionNum));

	return json(versions);
};

export const POST: RequestHandler = async ({ params, request }) => {
	const { content } = await request.json();

	const [latestVersion] = await db
		.select({ versionNum: noteVersions.versionNum })
		.from(noteVersions)
		.where(eq(noteVersions.noteId, params.id))
		.orderBy(desc(noteVersions.versionNum))
		.limit(1);

	const newVersionNum = (latestVersion?.versionNum ?? 0) + 1;

	const [newVersion] = await db
		.insert(noteVersions)
		.values({
			noteId: params.id,
			content: content ?? '',
			versionNum: newVersionNum
		})
		.returning();

	return json(newVersion);
};

import { sql } from 'drizzle-orm';
import { db } from './index';

async function waitForDatabase(maxRetries = 10, delayMs = 1000): Promise<void> {
	for (let i = 0; i < maxRetries; i++) {
		try {
			await db.execute(sql`SELECT 1`);
			return;
		} catch (error) {
			if (i === maxRetries - 1) throw error;
			console.log(`Waiting for database... (${i + 1}/${maxRetries})`);
			await new Promise((resolve) => setTimeout(resolve, delayMs));
		}
	}
}

export async function setupDatabase() {
	await waitForDatabase();

	await db.execute(sql`CREATE EXTENSION IF NOT EXISTS pg_xpatch`);

	await db.execute(sql`
		CREATE TABLE IF NOT EXISTS notes (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			title TEXT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		)
	`);

	await db.execute(sql`
		CREATE TABLE IF NOT EXISTS note_versions (
			id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
			note_id UUID NOT NULL REFERENCES notes(id) ON DELETE CASCADE,
			content TEXT NOT NULL,
			version_num INT NOT NULL,
			created_at TIMESTAMPTZ DEFAULT NOW()
		) USING xpatch
	`);

	const [existing] = await db.execute(sql`
		SELECT 1 FROM xpatch.table_config WHERE table_name = 'note_versions' LIMIT 1
	`);

	if (!existing) {
		// keyframe_every: Full snapshot interval. Higher = better compression, slower random access.
		// compress_depth: How many previous versions to compare. Higher = better compression, slower writes.
		await db.execute(sql`
			SELECT xpatch.configure(
				'note_versions'::regclass,
				group_by => 'note_id',
				order_by => 'version_num',
				delta_columns => ARRAY['content'],
				keyframe_every => 200,
				compress_depth => 100,
				enable_zstd => true
			)
		`);
	}
}

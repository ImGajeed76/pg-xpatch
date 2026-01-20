import { json } from '@sveltejs/kit';
import { db } from '$lib/server/db';
import { sql } from 'drizzle-orm';
import type { RequestHandler } from './$types';

export const GET: RequestHandler = async () => {
	try {
		const [stats] = await db.execute(sql`SELECT * FROM xpatch_stats('note_versions'::regclass)`);

		if (!stats) {
			return json(null);
		}

		// xpatch_stats().raw_size_bytes can return 0 in some cases, so we calculate it directly
		const [rawSize] = await db.execute(
			sql`SELECT COALESCE(SUM(length(content)), 0) as raw_bytes FROM note_versions`
		);

		const rawBytes = Number(rawSize?.raw_bytes ?? 0);
		const compressedBytes = Number(stats.compressed_size_bytes);

		return json({
			totalRows: Number(stats.total_rows),
			totalGroups: Number(stats.total_groups),
			keyframeCount: Number(stats.keyframe_count),
			deltaCount: Number(stats.delta_count),
			rawSizeBytes: rawBytes,
			compressedSizeBytes: compressedBytes,
			compressionRatio: compressedBytes > 0 ? rawBytes / compressedBytes : 0,
			cacheHits: Number(stats.cache_hits),
			cacheMisses: Number(stats.cache_misses),
			avgChainLength: Number(stats.avg_chain_length)
		});
	} catch {
		return json(null);
	}
};

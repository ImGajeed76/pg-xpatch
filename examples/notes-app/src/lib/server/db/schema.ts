import { pgTable, uuid, text, timestamp, integer } from 'drizzle-orm/pg-core';

export const notes = pgTable('notes', {
	id: uuid('id').primaryKey().defaultRandom(),
	title: text('title').notNull(),
	createdAt: timestamp('created_at', { withTimezone: true }).defaultNow()
});

export const noteVersions = pgTable('note_versions', {
	id: uuid('id').primaryKey().defaultRandom(),
	noteId: uuid('note_id')
		.notNull()
		.references(() => notes.id, { onDelete: 'cascade' }),
	content: text('content').notNull(),
	versionNum: integer('version_num').notNull(),
	createdAt: timestamp('created_at', { withTimezone: true }).defaultNow()
});

export type Note = typeof notes.$inferSelect;
export type NewNote = typeof notes.$inferInsert;
export type NoteVersion = typeof noteVersions.$inferSelect;
export type NewNoteVersion = typeof noteVersions.$inferInsert;

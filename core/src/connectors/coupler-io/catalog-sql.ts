/**
 * Sentinel catalog statements for the Coupler.io connector, answered by the CLI
 * executor from the Public API without calling /query. Kept apart from ./sql so
 * the metadata loader (which also runs in the browser) doesn't pull in the
 * statement parser for two constants.
 */

export const COUPLER_IO_TABLES_SQL = 'SELECT name, rows FROM coupler_io_catalog.tables';
export const COUPLER_IO_COLUMNS_SQL =
	'SELECT "tableName", "columnName", "columnType" FROM coupler_io_catalog.columns';

export function isCouplerIoCatalogSql(sql: string): 'tables' | 'columns' | null {
	if (sql.includes('coupler_io_catalog.tables')) return 'tables';
	if (sql.includes('coupler_io_catalog.columns')) return 'columns';
	return null;
}

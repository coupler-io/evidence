import { describe, it, expect } from 'vitest';
import {
	columnsFromDescribe,
	couplerIoDuckType,
	findTableRefs,
	inferColumns,
	slugifyTableName,
	typedViewSql,
	viewColumnsFromApi,
	withTableCte
} from './sql';
import { COUPLER_IO_COLUMNS_SQL, COUPLER_IO_TABLES_SQL, isCouplerIoCatalogSql } from './catalog-sql';

describe('couplerIoDuckType', () => {
	it('maps API column schemas to DuckDB types', () => {
		expect(couplerIoDuckType({ type: 'integer' })).toBe('BIGINT');
		expect(couplerIoDuckType({ type: 'number' })).toBe('DOUBLE');
		expect(couplerIoDuckType({ type: 'boolean' })).toBe('BOOLEAN');
		expect(couplerIoDuckType({ type: 'string', format: 'date' })).toBe('DATE');
		expect(couplerIoDuckType({ type: 'string', format: 'date-time' })).toBe('TIMESTAMP');
		expect(couplerIoDuckType({ type: 'string' })).toBe('VARCHAR');
		expect(couplerIoDuckType(undefined)).toBe('VARCHAR');
	});
});

describe('typedViewSql', () => {
	it('decodes and casts each column', () => {
		const sql = typedViewSql(
			viewColumnsFromApi([
				{ dbName: 'country', schema: { type: 'string' } },
				{ dbName: 'revenue', schema: { type: 'number' } },
				{ dbName: 'month', schema: { type: 'string', format: 'date' } },
				{ key: 'no_db_name' }
			])
		);
		expect(sql).toBe(
			`SELECT CASE WHEN json_valid(CAST("country" AS VARCHAR)) THEN json_extract_string(CAST("country" AS VARCHAR), '$') ELSE CAST("country" AS VARCHAR) END AS "country", ` +
				`TRY_CAST(CASE WHEN json_valid(CAST("revenue" AS VARCHAR)) THEN json_extract_string(CAST("revenue" AS VARCHAR), '$') ELSE CAST("revenue" AS VARCHAR) END AS DOUBLE) AS "revenue", ` +
				`TRY_CAST(CASE WHEN json_valid(CAST("month" AS VARCHAR)) THEN json_extract_string(CAST("month" AS VARCHAR), '$') ELSE CAST("month" AS VARCHAR) END AS DATE) AS "month" FROM data`
		);
	});

	it('falls back to SELECT * without a column schema', () => {
		expect(typedViewSql([])).toBe('SELECT * FROM data');
	});

	it('quotes identifiers containing quotes', () => {
		expect(typedViewSql([{ name: 'a"b', duckType: 'VARCHAR' }])).toContain('AS "a""b"');
	});
});

describe('findTableRefs', () => {
	const tables = ['funnel', 'Revenue'];

	it('finds tables after FROM and JOIN, case-insensitively', () => {
		expect(findTableRefs('select * from FUNNEL', tables)).toEqual(['funnel']);
		expect(findTableRefs('select * from x join revenue r on true', tables)).toEqual(['Revenue']);
		expect(findTableRefs('select * from "funnel" f', tables)).toEqual(['funnel']);
	});

	it('finds tables inside subqueries and CTEs', () => {
		expect(
			findTableRefs('with a as (select * from funnel) select * from (select * from a) t', tables)
		).toEqual(['funnel']);
	});

	it('reports every distinct table read', () => {
		expect(findTableRefs('select * from funnel join revenue using (id)', tables)).toEqual([
			'funnel',
			'Revenue'
		]);
	});

	it('ignores strings, comments, columns, qualified names, and table functions', () => {
		expect(findTableRefs("select 'from funnel' as s", tables)).toEqual([]);
		expect(findTableRefs('select 1 -- from funnel', tables)).toEqual([]);
		expect(findTableRefs('select 1 /* from funnel */', tables)).toEqual([]);
		expect(findTableRefs('select funnel from t', tables)).toEqual([]);
		expect(findTableRefs('select * from other.funnel', tables)).toEqual([]);
		expect(findTableRefs('select * from funnel.t', tables)).toEqual([]);
		expect(findTableRefs('select * from funnel(1)', tables)).toEqual([]);
	});

	it('finds tables in a comma-separated FROM list', () => {
		expect(findTableRefs('select * from calendar c, funnel f where c.d = f.d', tables)).toEqual([
			'funnel'
		]);
		expect(findTableRefs('select * from funnel, "revenue"', tables)).toEqual(['funnel', 'Revenue']);
		expect(findTableRefs('select * from a join b on a.x = b.x, funnel', tables)).toEqual([
			'funnel'
		]);
		expect(
			findTableRefs('select * from (select a, b from x), funnel where y in (1, 2)', tables)
		).toEqual(['funnel']);
	});

	it('ignores commas outside a FROM list', () => {
		expect(findTableRefs('select a, funnel from t', tables)).toEqual([]);
		expect(findTableRefs('select * from t where x in (a, funnel)', tables)).toEqual([]);
		expect(findTableRefs('select * from t group by a, funnel', tables)).toEqual([]);
		expect(findTableRefs('select * from t order by a, funnel', tables)).toEqual([]);
		expect(findTableRefs('select * from t join u using (a, funnel)', tables)).toEqual([]);
		expect(findTableRefs('select coalesce(a, funnel) from t', tables)).toEqual([]);
	});

	it('ignores names the statement defines as its own CTE', () => {
		expect(findTableRefs('with funnel as (select 1) select * from funnel', tables)).toEqual([]);
		expect(
			findTableRefs('with a as (select 1), funnel as (select 2) select * from funnel', tables)
		).toEqual([]);
		expect(
			findTableRefs('with funnel as materialized (select 1) select * from funnel', tables)
		).toEqual([]);
		expect(
			findTableRefs('with funnel as not materialized (select 1) select * from funnel', tables)
		).toEqual([]);
		expect(
			findTableRefs('with funnel(a, b) as (select 1, 2) select * from funnel', tables)
		).toEqual([]);
		expect(
			findTableRefs('with recursive "funnel" as (select 1) select * from funnel', tables)
		).toEqual([]);
	});

	it('does not mistake casts or aliases for CTE definitions', () => {
		expect(findTableRefs('select cast(x as varchar) as funnel from funnel', tables)).toEqual([
			'funnel'
		]);
	});
});

describe('withTableCte', () => {
	const view = 'SELECT 1 AS x FROM data';

	it('adds a WITH clause', () => {
		expect(withTableCte('select * from funnel', 'funnel', view)).toBe(
			'WITH "funnel" AS MATERIALIZED (SELECT 1 AS x FROM data) select * from funnel'
		);
	});

	it('keeps leading comments in place', () => {
		expect(withTableCte('-- c\nselect 1', 't', view)).toBe(
			'-- c\nWITH "t" AS MATERIALIZED (SELECT 1 AS x FROM data) select 1'
		);
	});

	it('merges into an existing WITH / WITH RECURSIVE', () => {
		expect(withTableCte('with a as (select 1) select * from a', 't', view)).toBe(
			'with "t" AS MATERIALIZED (SELECT 1 AS x FROM data), a as (select 1) select * from a'
		);
		expect(withTableCte('WITH RECURSIVE a as (select 1) select 1', 't', view)).toBe(
			'WITH RECURSIVE "t" AS MATERIALIZED (SELECT 1 AS x FROM data), a as (select 1) select 1'
		);
	});

	it('puts the CTE after DESCRIBE', () => {
		expect(withTableCte('DESCRIBE select * from t', 't', view)).toBe(
			'DESCRIBE WITH "t" AS MATERIALIZED (SELECT 1 AS x FROM data) select * from t'
		);
	});
});

describe('inferColumns', () => {
	it('types columns from the first non-null value', () => {
		const cols = inferColumns([
			{
				a: null,
				b: 'x',
				c: '2025-01-01T00:00:00.000Z',
				d: true,
				e: [1],
				f: null,
				g: 31
			},
			{ a: 1.5, b: 'y', c: null, d: false, e: null, f: null, g: 30 }
		]);
		expect(cols.map((c) => [c.name, c.jsType])).toEqual([
			['a', 'number'],
			['b', 'string'],
			['c', 'date'],
			['d', 'boolean'],
			['e', 'object'],
			['f', 'unknown'],
			['g', 'number']
		]);
		expect(cols.find((c) => c.name === 'a')?.clickhouseType).toBe('DOUBLE');
		expect(cols.find((c) => c.name === 'g')?.clickhouseType).toBe('BIGINT');
	});

	it('returns no columns for no rows', () => {
		expect(inferColumns([])).toEqual([]);
	});
});

describe('columnsFromDescribe', () => {
	it('maps DESCRIBE rows to columns', () => {
		expect(
			columnsFromDescribe([
				{ column_name: 'm', column_type: 'DATE', null: 'YES' },
				{ column_name: 'n', column_type: 'BIGINT', null: 'NO' }
			])
		).toEqual([
			{ name: 'm', clickhouseType: 'DATE', jsType: 'date', nullable: true },
			{
				name: 'n',
				clickhouseType: 'BIGINT',
				jsType: 'number',
				nullable: false
			}
		]);
	});
});

describe('isCouplerIoCatalogSql', () => {
	it('recognizes the catalog sentinels', () => {
		expect(isCouplerIoCatalogSql(COUPLER_IO_TABLES_SQL)).toBe('tables');
		expect(isCouplerIoCatalogSql(`${COUPLER_IO_COLUMNS_SQL} -- no cache`)).toBe('columns');
		expect(isCouplerIoCatalogSql('select 1')).toBeNull();
	});
});

describe('slugifyTableName', () => {
	it('derives SQL-friendly names', () => {
		expect(slugifyTableName('Monthly product funnel')).toBe('monthly_product_funnel');
		expect(slugifyTableName('Landing Pages (GA4 + GSC)')).toBe('landing_pages_ga4_gsc');
		expect(slugifyTableName('2024 sales')).toBe('_2024_sales');
		expect(slugifyTableName('!!!')).toBe('dataset');
	});
});

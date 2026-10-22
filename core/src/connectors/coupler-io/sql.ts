/**
 * Pure SQL helpers for the Coupler.io connector.
 *
 * Coupler.io's POST /query runs one DuckDB statement against one dataset,
 * exposed as the table `data`. Evidence SQL instead names datasets as tables
 * (`select … from funnel`), so the executor:
 *   1. finds which configured table the statement reads (findTableRefs),
 *   2. prepends that table as a CTE over `data` (withTableCte),
 *   3. sends it with the matching dataset id.
 *
 * Snapshot values are stored as VARCHAR holding JSON literals (`'"Italy"'`,
 * `'31'`), so the CTE is a typed view (typedViewSql) that decodes each value and
 * casts it to the column type the Public API reports for the dataset.
 */

import type { Column } from '../../user-components/interfaces/query-service';
import { getMotherduckToJsType } from '../motherduck/type-mapping';

/** Column shape from GET /v1/workspaces/{id}/datasets/{id} (`columns[]`). */
export type CouplerIoApiColumn = {
	key?: string;
	label?: string;
	dbName?: string;
	schema?: { type?: string; format?: string; nullable?: boolean };
};

export type CouplerIoViewColumn = { name: string; duckType: string };

/** Public API column schema (JSON-schema-ish type/format) → DuckDB type. */
export function couplerIoDuckType(schema: CouplerIoApiColumn['schema']): string {
	const type = schema?.type?.toLowerCase();
	const format = schema?.format?.toLowerCase();
	switch (type) {
		case 'integer':
			return 'BIGINT';
		case 'number':
			return 'DOUBLE';
		case 'boolean':
			return 'BOOLEAN';
		case 'string':
			if (format === 'date') return 'DATE';
			if (format === 'date-time' || format === 'datetime') return 'TIMESTAMP';
			return 'VARCHAR';
		default:
			return 'VARCHAR';
	}
}

export function quoteIdent(name: string): string {
	return `"${name.replace(/"/g, '""')}"`;
}

/** Visible API columns → typed view columns. Columns without a db name are skipped. */
export function viewColumnsFromApi(columns: CouplerIoApiColumn[]): CouplerIoViewColumn[] {
	return columns
		.filter((c) => typeof c.dbName === 'string' && c.dbName.length > 0)
		.map((c) => ({ name: c.dbName as string, duckType: couplerIoDuckType(c.schema) }));
}

function decodedExpr(name: string): string {
	// Cast to VARCHAR first so the expression works whatever the stored type is;
	// unwrap JSON literals, and leave plain (non-JSON) text unchanged.
	const v = `CAST(${quoteIdent(name)} AS VARCHAR)`;
	return `CASE WHEN json_valid(${v}) THEN json_extract_string(${v}, '$') ELSE ${v} END`;
}

function typedExpr(col: CouplerIoViewColumn): string {
	const decoded = decodedExpr(col.name);
	switch (col.duckType) {
		case 'VARCHAR':
			return decoded;
		case 'BIGINT':
			// Tolerate integral values serialized with a fraction ("31.0").
			return `COALESCE(TRY_CAST(${decoded} AS BIGINT), CAST(TRY_CAST(${decoded} AS DOUBLE) AS BIGINT))`;
		default:
			return `TRY_CAST(${decoded} AS ${col.duckType})`;
	}
}

/** SELECT over `data` that exposes each column decoded and typed. */
export function typedViewSql(columns: CouplerIoViewColumn[]): string {
	if (columns.length === 0) return 'SELECT * FROM data';
	const select = columns.map((c) => `${typedExpr(c)} AS ${quoteIdent(c.name)}`).join(', ');
	return `SELECT ${select} FROM data`;
}

// ---------------------------------------------------------------------------
// Tokenizer — just enough DuckDB lexing to tell identifiers apart from string
// literals, comments, and quoted identifiers.
// ---------------------------------------------------------------------------

type Token =
	| { kind: 'word'; value: string; start: number; end: number }
	| { kind: 'quoted'; value: string; start: number; end: number }
	| { kind: 'punct'; value: string; start: number; end: number };

function tokenize(sql: string): Token[] {
	const tokens: Token[] = [];
	let i = 0;
	const n = sql.length;
	while (i < n) {
		const ch = sql[i];
		if (/\s/.test(ch)) {
			i++;
		} else if (ch === '-' && sql[i + 1] === '-') {
			while (i < n && sql[i] !== '\n') i++;
		} else if (ch === '/' && sql[i + 1] === '*') {
			const close = sql.indexOf('*/', i + 2);
			i = close === -1 ? n : close + 2;
		} else if (ch === "'") {
			i++;
			while (i < n) {
				if (sql[i] === "'" && sql[i + 1] === "'") i += 2;
				else if (sql[i] === "'") break;
				else i++;
			}
			i++;
		} else if (ch === '"') {
			const start = i;
			let value = '';
			i++;
			while (i < n) {
				if (sql[i] === '"' && sql[i + 1] === '"') {
					value += '"';
					i += 2;
				} else if (sql[i] === '"') break;
				else value += sql[i++];
			}
			i++;
			tokens.push({ kind: 'quoted', value, start, end: i });
		} else if (/[A-Za-z_]/.test(ch)) {
			const start = i;
			while (i < n && /[A-Za-z0-9_$]/.test(sql[i])) i++;
			tokens.push({ kind: 'word', value: sql.slice(start, i), start, end: i });
		} else {
			tokens.push({ kind: 'punct', value: ch, start: i, end: i + 1 });
			i++;
		}
	}
	return tokens;
}

const isKeyword = (t: Token | undefined, kw: string) =>
	t?.kind === 'word' && t.value.toUpperCase() === kw;

const isPunct = (t: Token | undefined, p: string) => t?.kind === 'punct' && t.value === p;

/** Index just past the parenthesis group opening at `open`. */
function skipParens(tokens: Token[], open: number): number {
	let depth = 0;
	for (let i = open; i < tokens.length; i++) {
		if (isPunct(tokens[i], '(')) depth++;
		else if (isPunct(tokens[i], ')') && --depth === 0) return i + 1;
	}
	return tokens.length;
}

/**
 * Names the statement defines as CTEs: `name [(cols)] AS [NOT] [MATERIALIZED] (`
 * right after WITH, RECURSIVE, or a comma.
 */
function ownCteNames(tokens: Token[]): Set<string> {
	const names = new Set<string>();
	for (let i = 1; i < tokens.length; i++) {
		const t = tokens[i];
		if (t.kind !== 'word' && t.kind !== 'quoted') continue;
		const prev = tokens[i - 1];
		if (!isKeyword(prev, 'WITH') && !isKeyword(prev, 'RECURSIVE') && !isPunct(prev, ',')) continue;
		let j = i + 1;
		if (isPunct(tokens[j], '(')) j = skipParens(tokens, j);
		if (!isKeyword(tokens[j++], 'AS')) continue;
		if (isKeyword(tokens[j], 'NOT')) j++;
		if (isKeyword(tokens[j], 'MATERIALIZED')) j++;
		if (isPunct(tokens[j], '(')) names.add(t.value.toLowerCase());
	}
	return names;
}

// Keywords that end a FROM clause's comma-separated table list.
const FROM_CLAUSE_END = new Set([
	'WHERE',
	'GROUP',
	'HAVING',
	'QUALIFY',
	'WINDOW',
	'ORDER',
	'LIMIT',
	'OFFSET',
	'UNION',
	'INTERSECT',
	'EXCEPT',
	'SELECT'
]);

/**
 * Configured table names the statement reads: an identifier right after FROM,
 * JOIN, or a comma in a FROM list that isn't schema-qualified (`x.funnel`), a
 * schema (`funnel.x`), or a table function (`funnel(...)`). Names the statement
 * defines itself as a CTE are excluded. Returns the canonical (configured) names.
 */
export function findTableRefs(sql: string, tableNames: Iterable<string>): string[] {
	const byLower = new Map<string, string>();
	for (const name of tableNames) byLower.set(name.toLowerCase(), name);

	const tokens = tokenize(sql);
	const ownCtes = ownCteNames(tokens);

	// inFromList[depth]: a comma at this parenthesis depth separates FROM items.
	const inFromList: boolean[] = [false];
	const found = new Set<string>();
	for (let i = 0; i < tokens.length; i++) {
		const t = tokens[i];
		const depth = inFromList.length - 1;
		if (isPunct(t, '(')) inFromList.push(false);
		else if (isPunct(t, ')')) {
			if (depth > 0) inFromList.pop();
		} else if (isPunct(t, ';')) inFromList[depth] = false;
		else if (isKeyword(t, 'FROM')) inFromList[depth] = true;
		else if (t.kind === 'word' && FROM_CLAUSE_END.has(t.value.toUpperCase())) {
			inFromList[depth] = false;
		}

		if (i === 0 || (t.kind !== 'word' && t.kind !== 'quoted')) continue;
		const prev = tokens[i - 1];
		const afterFromListComma = isPunct(prev, ',') && inFromList[depth];
		if (!isKeyword(prev, 'FROM') && !isKeyword(prev, 'JOIN') && !afterFromListComma) continue;
		const next = tokens[i + 1];
		if (next?.value === '.' || next?.value === '(') continue;
		const key = t.value.toLowerCase();
		const canonical = byLower.get(key);
		if (canonical && !ownCtes.has(key)) found.add(canonical);
	}
	return [...found];
}

/**
 * Prepend `name AS (viewSql)` to the statement's WITH clause (or add one).
 * Statements that start with DESCRIBE/SUMMARIZE get the CTE on the query after it.
 */
export function withTableCte(sql: string, name: string, viewSql: string): string {
	// MATERIALIZED: Coupler.io's DuckDB (1.5.x) hits an internal error inlining a CTE
	// over the `data` scan when the statement is wrapped in a subquery (e.g.
	// `SELECT * FROM (…) LIMIT n`) and references the table more than once.
	const cte = `${quoteIdent(name)} AS MATERIALIZED (${viewSql})`;
	const tokens = tokenize(sql);
	let idx = 0;
	if (isKeyword(tokens[0], 'DESCRIBE') || isKeyword(tokens[0], 'SUMMARIZE')) idx = 1;
	const head = tokens[idx];
	if (!head) return `WITH ${cte} ${sql}`;

	if (isKeyword(head, 'WITH')) {
		const after = isKeyword(tokens[idx + 1], 'RECURSIVE') ? tokens[idx + 1] : head;
		return `${sql.slice(0, after.end)} ${cte},${sql.slice(after.end)}`;
	}
	return `${sql.slice(0, head.start)}WITH ${cte} ${sql.slice(head.start)}`;
}

// ---------------------------------------------------------------------------
// Result typing — /query returns rows only, so infer column types from values.
// ---------------------------------------------------------------------------

// /query serializes DATE/TIMESTAMP values as ISO instants (`2025-01-01T00:00:00.000Z`).
const ISO_INSTANT_RE = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(\.\d+)?Z$/;

/**
 * Column metadata inferred from row values, in first-seen key order. A column
 * whose values are all null comes back as `unknown` (the caller can DESCRIBE).
 */
export function inferColumns(rows: Record<string, unknown>[]): Column[] {
	const order: string[] = [];
	const seen = new Set<string>();
	for (const row of rows) {
		for (const key of Object.keys(row)) {
			if (!seen.has(key)) {
				seen.add(key);
				order.push(key);
			}
		}
	}
	return order.map((name) => {
		const sample = rows.find((r) => r[name] !== null && r[name] !== undefined)?.[name];
		let type = 'UNKNOWN';
		if (typeof sample === 'number') {
			// JSON drops the integer/float distinction; whole values in every row read as integers.
			const whole = rows.every(
				(r) => r[name] === null || r[name] === undefined || Number.isInteger(r[name])
			);
			type = whole ? 'BIGINT' : 'DOUBLE';
		} else if (typeof sample === 'boolean') type = 'BOOLEAN';
		else if (typeof sample === 'string')
			type = ISO_INSTANT_RE.test(sample) ? 'TIMESTAMP' : 'VARCHAR';
		else if (typeof sample === 'object' && sample !== null) type = 'JSON_OBJECT';
		return {
			name,
			clickhouseType: type,
			jsType: type === 'JSON_OBJECT' ? 'object' : getMotherduckToJsType(type)
		};
	});
}

/** Rows of `DESCRIBE <stmt>` (column_name, column_type) → Evidence columns. */
export function columnsFromDescribe(rows: Record<string, unknown>[]): Column[] {
	return rows.map((r) => {
		const type = String(r.column_type ?? 'UNKNOWN');
		return {
			name: String(r.column_name),
			clickhouseType: type,
			jsType: getMotherduckToJsType(type),
			nullable: r.null === undefined ? undefined : r.null === 'YES'
		};
	});
}

/** Dataset name → SQL-friendly table name (lowercase snake_case). */
export function slugifyTableName(name: string): string {
	const slug = name
		.toLowerCase()
		.normalize('NFKD')
		.replace(/[^a-z0-9]+/g, '_')
		.replace(/^_+|_+$/g, '');
	if (!slug) return 'dataset';
	return /^[0-9]/.test(slug) ? `_${slug}` : slug;
}

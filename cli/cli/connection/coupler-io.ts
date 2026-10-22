/**
 * Direct Coupler.io query execution over the Coupler.io Public API.
 *
 * Each configured table is one Coupler.io dataset. A statement may read at most
 * one of them: POST /query runs DuckDB SQL against a single dataset snapshot
 * (as the table `data`), so the statement is sent with that dataset's id and a
 * typed CTE named after the table (see @evidence/core/connectors/coupler-io/sql).
 *
 * Catalog (table list, dataset columns) and query results are cached per
 * process for a few minutes — the API's allowance is 100 requests/minute per
 * organization, and a page load runs every query on it.
 */

import {
	COUPLER_IO_COLUMNS_SQL,
	COUPLER_IO_TABLES_SQL,
	columnsFromDescribe,
	findTableRefs,
	inferColumns,
	isCouplerIoCatalogSql,
	slugifyTableName,
	typedViewSql,
	viewColumnsFromApi,
	withTableCte,
	type CouplerIoApiColumn,
	type CouplerIoViewColumn
} from '@evidence/core/connectors/coupler-io/sql';
import { normalizeDateRows } from '@evidence/core/connectors/postgres/normalize-date-rows';
import { normalizeSparklineRows } from '@evidence/core/connectors/normalize-sparkline-rows';
import type { CouplerIoCredentials } from '@evidence/core/connectors/coupler-io/credentials';
import type { Column } from '@evidence/core/user-components/interfaces/query-service';
import type { QueryResult } from './types';

export { COUPLER_IO_TABLES_SQL, COUPLER_IO_COLUMNS_SQL };

const CATALOG_TTL_MS = 5 * 60_000;

type CatalogEntry = { name: string; datasetId: string; rowsCount: number | null };

type DatasetResponse = {
	id: string;
	name: string;
	state?: string;
	queryable?: boolean;
	rows_count?: number;
	columns?: CouplerIoApiColumn[];
};

type Cached<T> = { at: number; value: Promise<T> };

class CouplerIoClient {
	#catalog: Cached<Map<string, CatalogEntry>> | null = null;
	#views = new Map<string, Cached<CouplerIoViewColumn[]>>();
	#results = new Map<string, Cached<QueryResult>>();
	#active = 0;
	#waiting: (() => void)[] = [];

	constructor(readonly config: CouplerIoCredentials) {}

	async #request<T>(url: string, init: RequestInit = {}): Promise<T> {
		// Small concurrency cap so a page's burst of queries doesn't trip the rate limit.
		if (this.#active >= this.config.maxConcurrentRequests) {
			await new Promise<void>((resolve) => this.#waiting.push(resolve));
		}
		this.#active++;
		try {
			const response = await fetch(url, {
				...init,
				headers: {
					Authorization: `Bearer ${this.config.token}`,
					'Content-Type': 'application/json',
					Accept: 'application/json'
				}
			});
			const text = await response.text();
			if (!response.ok) throw new Error(errorMessage(response.status, text));
			return JSON.parse(text) as T;
		} finally {
			this.#active--;
			this.#waiting.shift()?.();
		}
	}

	#api<T>(path: string): Promise<T> {
		return this.#request<T>(
			`${this.config.apiHost}/v1/workspaces/${encodeURIComponent(this.config.workspaceId)}${path}`
		);
	}

	async query(datasetId: string, statement: string) {
		return this.#request<{ rows: Record<string, unknown>[]; truncated: boolean }>(
			`${this.config.queryHost}/query`,
			{
				method: 'POST',
				body: JSON.stringify({
					workspace_id: this.config.workspaceId,
					dataset_id: datasetId,
					statement
				})
			}
		);
	}

	catalog(): Promise<Map<string, CatalogEntry>> {
		if (this.#catalog && Date.now() - this.#catalog.at < CATALOG_TTL_MS) return this.#catalog.value;
		const value = this.#loadCatalog();
		this.#catalog = { at: Date.now(), value };
		value.catch(() => (this.#catalog = null));
		return value;
	}

	async #loadCatalog(): Promise<Map<string, CatalogEntry>> {
		const entries = new Map<string, CatalogEntry>();
		const explicit = Object.entries(this.config.tables);
		if (explicit.length > 0) {
			for (const [name, datasetId] of explicit) {
				entries.set(name.toLowerCase(), { name, datasetId, rowsCount: null });
			}
			return entries;
		}

		// Auto-discovery: every queryable dataset, named after the dataset. The API
		// returns at most 200, newest successful run first.
		const list = await this.#api<{ datasets: DatasetResponse[] }>('/datasets');
		for (const d of list.datasets.filter((d) => d.queryable)) {
			let name = slugifyTableName(d.name);
			if (entries.has(name)) name = `${name}_${d.id.slice(0, 8).toLowerCase()}`;
			entries.set(name, { name, datasetId: d.id, rowsCount: d.rows_count ?? null });
		}
		return entries;
	}

	/** Memoize a query result by dataset + statement (in-flight requests are shared too). */
	cachedResult(datasetId: string, statement: string, run: () => Promise<QueryResult>) {
		const key = JSON.stringify([datasetId, statement]);
		const cached = this.#results.get(key);
		if (cached && Date.now() - cached.at < CATALOG_TTL_MS) return cached.value;
		const value = run();
		this.#results.set(key, { at: Date.now(), value });
		value.catch(() => this.#results.delete(key));
		return value;
	}

	viewColumns(datasetId: string): Promise<CouplerIoViewColumn[]> {
		const cached = this.#views.get(datasetId);
		if (cached && Date.now() - cached.at < CATALOG_TTL_MS) return cached.value;
		const value = this.#api<DatasetResponse>(`/datasets/${encodeURIComponent(datasetId)}`).then(
			(d) => {
				if (d.queryable === false) {
					throw new Error(
						`Coupler.io dataset "${d.name}" (${datasetId}) is not queryable (state: ${d.state}). Share it for analysis in the dataflow destination, or run the dataflow.`
					);
				}
				return viewColumnsFromApi(d.columns ?? []);
			}
		);
		this.#views.set(datasetId, { at: Date.now(), value });
		value.catch(() => this.#views.delete(datasetId));
		return value;
	}
}

function errorMessage(status: number, body: string): string {
	let message = body;
	try {
		message = JSON.parse(body)?.error?.message ?? body;
	} catch {
		// Non-JSON body — keep it verbatim.
	}
	if (status === 401) return `Coupler.io: API token is missing or invalid. ${message}`;
	if (status === 429) {
		return 'Coupler.io: API rate limit exceeded (100 requests/minute per organization). Wait a minute and retry.';
	}
	return `Coupler.io (${status}): ${message}`;
}

let cachedClient: { key: string; client: CouplerIoClient } | null = null;

function getClient(config: CouplerIoCredentials): CouplerIoClient {
	// JSON.stringify (not join) so a value containing a delimiter can't collide.
	const key = JSON.stringify([
		config.apiHost,
		config.queryHost,
		config.workspaceId,
		config.token,
		config.tables,
		config.maxConcurrentRequests
	]);
	if (cachedClient?.key === key) return cachedClient.client;
	cachedClient = { key, client: new CouplerIoClient(config) };
	return cachedClient.client;
}

async function catalogResult(
	client: CouplerIoClient,
	kind: 'tables' | 'columns'
): Promise<QueryResult> {
	const catalog = [...(await client.catalog()).values()];
	if (kind === 'tables') {
		return {
			rows: catalog.map((t) => ({ name: t.name, rows: t.rowsCount })),
			columns: [
				{ name: 'name', clickhouseType: 'VARCHAR', jsType: 'string' },
				{ name: 'rows', clickhouseType: 'BIGINT', jsType: 'number' }
			]
		};
	}
	const rows: Record<string, unknown>[] = [];
	for (const table of catalog) {
		let columns: CouplerIoViewColumn[];
		try {
			columns = await client.viewColumns(table.datasetId);
		} catch {
			continue; // An unreadable dataset shouldn't hide the rest of the catalog.
		}
		for (const c of columns) {
			rows.push({ tableName: table.name, columnName: c.name, columnType: c.duckType });
		}
	}
	return {
		rows,
		columns: ['tableName', 'columnName', 'columnType'].map((name) => ({
			name,
			clickhouseType: 'VARCHAR',
			jsType: 'string' as const
		}))
	};
}

export async function executeCouplerIoQuery(
	sql: string,
	config: CouplerIoCredentials
): Promise<QueryResult> {
	const client = getClient(config);

	const catalogKind = isCouplerIoCatalogSql(sql);
	if (catalogKind) return catalogResult(client, catalogKind);

	const catalog = await client.catalog();
	if (catalog.size === 0) {
		throw new Error(
			`Coupler.io: no queryable datasets in workspace ${config.workspaceId}. Add a \`datasets:\` map to connection.yaml.`
		);
	}

	const refs = findTableRefs(
		sql,
		[...catalog.values()].map((t) => t.name)
	);
	if (refs.length > 1) {
		throw new Error(
			`Coupler.io can only query one dataset per statement, but this one reads ${refs.join(', ')}. Join the datasets in a Coupler.io dataflow instead.`
		);
	}

	let statement = sql;
	let datasetId: string;
	if (refs.length === 1) {
		const table = catalog.get(refs[0].toLowerCase())!;
		datasetId = table.datasetId;
		statement = withTableCte(sql, table.name, typedViewSql(await client.viewColumns(datasetId)));
	} else {
		// Reads no dataset (e.g. `select 1`): any dataset can host the statement.
		datasetId = catalog.values().next().value!.datasetId;
	}

	const result = await client.cachedResult(datasetId, statement, () =>
		runStatement(client, datasetId, statement)
	);
	// Callers may mutate rows; hand each one its own copy of the cached result.
	return { rows: result.rows.map((r) => ({ ...r })), columns: result.columns };
}

// Evidence's column probe: `SELECT * FROM (…) alias WHERE 1=0`.
const ZERO_ROW_PROBE_RE = /\bWHERE\s+1\s*=\s*0\s*$/i;

async function runStatement(
	client: CouplerIoClient,
	datasetId: string,
	statement: string
): Promise<QueryResult> {
	// A probe has no rows by construction, so skip straight to DESCRIBE.
	if (ZERO_ROW_PROBE_RE.test(statement)) {
		const described = await client.query(datasetId, `DESCRIBE ${statement}`);
		return { rows: [], columns: columnsFromDescribe(described.rows ?? []) };
	}

	const result = await client.query(datasetId, statement);
	const rows = result.rows ?? [];
	if (result.truncated) {
		console.warn(
			'[coupler-io] result truncated to 10,000 rows — aggregate, filter, or add LIMIT for a complete result.'
		);
	}

	let columns: Column[] = inferColumns(rows);
	// Values can't type a column with no rows or only nulls (e.g. a filter that
	// matches nothing); ask DuckDB for the result schema instead.
	if (columns.length === 0 || columns.some((c) => c.jsType === 'unknown')) {
		const described = await client.query(datasetId, `DESCRIBE ${statement}`);
		columns = columnsFromDescribe(described.rows ?? []);
	}

	const dateColumns = new Set(columns.filter((c) => c.jsType === 'date').map((c) => c.name));
	normalizeDateRows(rows, dateColumns);
	normalizeSparklineRows(rows, columns);

	return { rows, columns };
}

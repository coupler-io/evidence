import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { CouplerIoCredentials } from '@evidence/core/connectors/coupler-io/credentials';
import { COUPLER_IO_COLUMNS_SQL, CouplerIoClient, executeCouplerIoQuery } from './coupler-io';

let tokenSeq = 0;
// A fresh token per test so executeCouplerIoQuery doesn't reuse another test's client.
const config = (extra: Partial<CouplerIoCredentials> = {}): CouplerIoCredentials => ({
	token: `t${++tokenSeq}`,
	workspaceId: '9',
	apiHost: 'https://api.test',
	queryHost: 'https://query.test',
	tables: { funnel: 'ds-funnel' },
	maxConcurrentRequests: 4,
	...extra
});

const json = (body: unknown, status = 200, headers: Record<string, string> = {}) =>
	new Response(JSON.stringify(body), { status, headers });

const queryOk = () => json({ rows: [{ x: 1 }], truncated: false });

/** Resolve every pending fetch/microtask without moving the clock. */
const flush = () => vi.advanceTimersByTimeAsync(0);

describe('CouplerIoClient request scheduling', () => {
	let fetchMock: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		vi.useFakeTimers();
		fetchMock = vi.fn();
		vi.stubGlobal('fetch', fetchMock);
		vi.spyOn(console, 'warn').mockImplementation(() => {});
	});

	afterEach(() => {
		vi.useRealTimers();
		vi.unstubAllGlobals();
		vi.restoreAllMocks();
	});

	it('retries a 429 after Retry-After and pauses other requests meanwhile', async () => {
		fetchMock.mockResolvedValueOnce(
			json({ error: { message: 'slow down' } }, 429, { 'Retry-After': '2' })
		);
		fetchMock.mockImplementation(async () => queryOk());
		const client = new CouplerIoClient(config());

		const first = client.query('ds', 'select 1');
		await flush();
		const second = client.query('ds', 'select 2');
		await flush();
		expect(fetchMock).toHaveBeenCalledTimes(1);

		await vi.advanceTimersByTimeAsync(2000);
		await expect(first).resolves.toEqual({ rows: [{ x: 1 }], truncated: false });
		await expect(second).resolves.toBeDefined();
		expect(fetchMock).toHaveBeenCalledTimes(3);
	});

	it('backs off 15s, 30s, 60s without Retry-After, then gives up', async () => {
		fetchMock.mockImplementation(async () => json({ error: { message: 'slow down' } }, 429));
		const client = new CouplerIoClient(config());

		const result = client.query('ds', 'select 1');
		const assertion = expect(result).rejects.toThrow(/rate limit exceeded/);
		await flush();
		expect(fetchMock).toHaveBeenCalledTimes(1);
		await vi.advanceTimersByTimeAsync(15_000);
		expect(fetchMock).toHaveBeenCalledTimes(2);
		await vi.advanceTimersByTimeAsync(30_000);
		expect(fetchMock).toHaveBeenCalledTimes(3);
		await vi.advanceTimersByTimeAsync(59_999);
		expect(fetchMock).toHaveBeenCalledTimes(3);
		await vi.advanceTimersByTimeAsync(1);
		expect(fetchMock).toHaveBeenCalledTimes(4);
		await assertion;
	});

	it('starts at most 90 requests per minute', async () => {
		fetchMock.mockImplementation(async () => queryOk());
		const client = new CouplerIoClient(config({ maxConcurrentRequests: 50 }));

		const all = Array.from({ length: 95 }, (_, i) => client.query('ds', `select ${i}`));
		await flush();
		expect(fetchMock).toHaveBeenCalledTimes(90);
		await vi.advanceTimersByTimeAsync(59_999);
		expect(fetchMock).toHaveBeenCalledTimes(90);
		await vi.advanceTimersByTimeAsync(1);
		expect(fetchMock).toHaveBeenCalledTimes(95);
		await Promise.all(all);
	});

	it('never exceeds maxConcurrentRequests in flight', async () => {
		let inFlight = 0;
		let peak = 0;
		fetchMock.mockImplementation(async () => {
			peak = Math.max(peak, ++inFlight);
			await new Promise((r) => setTimeout(r, 100));
			inFlight--;
			return queryOk();
		});
		const client = new CouplerIoClient(config({ maxConcurrentRequests: 2 }));

		const all = Array.from({ length: 6 }, (_, i) => client.query('ds', `select ${i}`));
		await vi.advanceTimersByTimeAsync(1000);
		await Promise.all(all);
		expect(peak).toBe(2);
		expect(fetchMock).toHaveBeenCalledTimes(6);
	});

	it('runs queued queries before queued schema-browser lookups', async () => {
		const order: string[] = [];
		fetchMock.mockImplementation(async (url: string) => {
			order.push(url.includes('/query') ? 'query' : url.split('/').pop()!);
			await new Promise((r) => setTimeout(r, 10));
			return url.includes('/query') ? queryOk() : json({ id: 'x', name: 'x', columns: [] });
		});
		const client = new CouplerIoClient(config({ maxConcurrentRequests: 1 }));

		const lookups = ['a', 'b', 'c'].map((id) => client.viewColumns(id, 'low'));
		await flush();
		const query = client.query('ds', 'select 1');
		await vi.advanceTimersByTimeAsync(100);
		await Promise.all([...lookups, query]);
		expect(order).toEqual(['a', 'query', 'b', 'c']);
	});

	it('does not make a query wait behind a still-queued low-priority lookup', async () => {
		const order: string[] = [];
		fetchMock.mockImplementation(async (url: string) => {
			order.push(url.split('/').pop()!);
			await new Promise((r) => setTimeout(r, 10));
			return json({ id: 'x', name: 'x', columns: [{ dbName: 'c', schema: { type: 'integer' } }] });
		});
		const client = new CouplerIoClient(config({ maxConcurrentRequests: 1 }));

		const lookups = ['a', 'b', 'c'].map((id) => client.viewColumns(id, 'low'));
		await flush();
		const forQuery = client.viewColumns('c');
		await vi.advanceTimersByTimeAsync(100);
		await expect(forQuery).resolves.toEqual([{ name: 'c', duckType: 'BIGINT' }]);
		await Promise.all(lookups);
		expect(order.slice(0, 2)).toEqual(['a', 'c']);
	});
});

describe('executeCouplerIoQuery schema catalog', () => {
	let fetchMock: ReturnType<typeof vi.fn>;

	beforeEach(() => {
		fetchMock = vi.fn();
		vi.stubGlobal('fetch', fetchMock);
		vi.spyOn(console, 'warn').mockImplementation(() => {});
	});

	afterEach(() => {
		vi.unstubAllGlobals();
		vi.restoreAllMocks();
	});

	const dataset = (id: string, extra: object = {}) =>
		json({
			id,
			name: id,
			queryable: true,
			columns: [{ dbName: 'v', schema: { type: 'number' } }],
			...extra
		});

	it('fetches every table schema concurrently and skips unreadable datasets', async () => {
		let inFlight = 0;
		let peak = 0;
		fetchMock.mockImplementation(async (url: string) => {
			peak = Math.max(peak, ++inFlight);
			await new Promise((r) => setTimeout(r, 5));
			inFlight--;
			const id = url.split('/').pop()!;
			if (id === 'ds-hidden') return dataset(id, { queryable: false, state: 'FULL' });
			if (id === 'ds-gone') return json({ error: { message: 'TransformationSet not found' } }, 404);
			return dataset(id);
		});

		const result = await executeCouplerIoQuery(
			COUPLER_IO_COLUMNS_SQL,
			config({ tables: { a: 'ds-a', b: 'ds-b', hidden: 'ds-hidden', gone: 'ds-gone' } })
		);

		expect(result.rows).toEqual([
			{ tableName: 'a', columnName: 'v', columnType: 'DOUBLE' },
			{ tableName: 'b', columnName: 'v', columnType: 'DOUBLE' }
		]);
		expect(peak).toBe(4);
		expect(console.warn).toHaveBeenCalledTimes(2);
	});

	it('fails on auth errors instead of returning an empty schema', async () => {
		fetchMock.mockImplementation(async () =>
			json({ error: { message: 'Credentials may be invalid' } }, 401)
		);
		await expect(
			executeCouplerIoQuery(COUPLER_IO_COLUMNS_SQL, config({ tables: { a: 'ds-a' } }))
		).rejects.toThrow(/API token is missing or invalid/);
	});
});

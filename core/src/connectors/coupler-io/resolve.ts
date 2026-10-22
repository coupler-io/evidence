import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { z } from 'zod/v4';
import {
	COUPLER_IO_DEFAULT_API_HOST,
	COUPLER_IO_DEFAULT_QUERY_HOST,
	couplerIoOrigin,
	type CouplerIoConnection
} from './connection-schema';
import type { CouplerIoCredentials } from './credentials';

export type ResolveOpts = {
	/** Base directory for resolving the `keyfile` path. */
	cwd: string;
};

/** Shape of the `keyfile` JSON. Hosts are optional; connection.yaml values win. */
const keyfileSchema = z.object({
	token: z.string().min(1),
	api_host: couplerIoOrigin.optional(),
	query_host: couplerIoOrigin.optional()
});

async function readKeyfile(file: string, cwd: string): Promise<z.infer<typeof keyfileSchema>> {
	const resolved = path.isAbsolute(file) ? file : path.join(cwd, file);
	let parsed: unknown;
	try {
		parsed = JSON.parse(await readFile(resolved, 'utf-8'));
	} catch (e) {
		throw new Error(`failed to read keyfile "${file}": ${(e as Error).message}`);
	}
	const result = keyfileSchema.safeParse(parsed);
	if (!result.success) {
		const issue = result.error.issues[0];
		throw new Error(`keyfile "${file}": ${issue?.path.join('.') || 'value'}: ${issue?.message}`);
	}
	return result.data;
}

/**
 * Config layer → execution layer. Reads the key file (if any) at the trust
 * boundary; hosts resolve connection.yaml → key file → production default.
 */
export async function resolveCouplerIoCredentials(
	config: CouplerIoConnection,
	opts: ResolveOpts
): Promise<CouplerIoCredentials> {
	const keyfile = config.keyfile ? await readKeyfile(config.keyfile, opts.cwd) : undefined;
	return {
		token: config.token ?? keyfile!.token,
		workspaceId: config.workspace_id,
		apiHost: config.api_host ?? keyfile?.api_host ?? COUPLER_IO_DEFAULT_API_HOST,
		queryHost: config.query_host ?? keyfile?.query_host ?? COUPLER_IO_DEFAULT_QUERY_HOST,
		tables: config.datasets,
		maxConcurrentRequests: config.max_concurrent_requests
	};
}

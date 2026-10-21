import { z } from 'zod/v4';
import { authGroupOneOfCheck, type ConnectionFieldMeta } from '../connection-schema';

const meta = (m: ConnectionFieldMeta): ConnectionFieldMeta => m;

export const COUPLER_IO_DEFAULT_API_HOST = 'https://api.coupler.io';
export const COUPLER_IO_DEFAULT_QUERY_HOST = 'https://services.coupler.io';

export const httpsUrl = z
	.string()
	.min(1)
	.transform((s) => s.trim().replace(/\/+$/, ''))
	.refine((s) => /^https:\/\/[^/\s]+$/i.test(s), {
		message: 'should be an https:// origin with no path, e.g. https://api.coupler.io'
	});

/**
 * Coupler.io direct connector. Queries go over the Coupler.io Public API: each
 * statement runs (as DuckDB SQL) against the latest snapshot of exactly one
 * dataset. Datasets are exposed as tables — either an explicit alias → dataset
 * id map, or every queryable dataset in the workspace under a name derived
 * from the dataset name.
 */
export const couplerIoBase = z.object({
	type: z.literal('coupler-io'),

	token: z
		.string()
		.min(1)
		.optional()
		.meta(
			meta({
				label: 'API token',
				description:
					'Personal API token from the API access section of the Coupler.io user profile. Sent as a Bearer token.',
				category: 'credential',
				secret: true,
				authGroup: 'auth'
			})
		),

	keyfile: z
		.string()
		.min(1)
		.optional()
		.meta(
			meta({
				label: 'Key file path',
				description:
					'Path to a JSON file with the API token, resolved relative to connection.yaml: {"token": "...", "api_host": "...", "query_host": "..."} (hosts optional).',
				category: 'credential',
				cliOnly: true,
				fileRef: { resolveRelativeTo: 'cwd' },
				authGroup: 'auth'
			})
		),

	workspace_id: z
		.union([z.string().min(1), z.number().int()])
		.transform((v) => String(v))
		.meta(
			meta({
				label: 'Workspace ID',
				description: 'Coupler.io workspace that owns the datasets (GET /v1/workspaces lists them).',
				category: 'context'
			})
		),

	// No zod defaults on the hosts: a key file may set them, so resolve.ts applies
	// connection.yaml → key file → production default.
	api_host: httpsUrl.optional().meta(
		meta({
			label: 'API host',
			description:
				'Coupler.io Public API origin. Defaults to the key file value, then production; use https://api.couplerstaging.dev for staging.',
			category: 'context'
		})
	),

	query_host: httpsUrl.optional().meta(
		meta({
			label: 'Query host',
			description:
				'Origin serving POST /query. Defaults to the key file value, then production; use https://services.couplerstaging.dev for staging.',
			category: 'context'
		})
	),

	datasets: z
		.record(
			z.string().regex(/^[A-Za-z_][A-Za-z0-9_]*$/, {
				message: 'table names must be simple identifiers (letters, digits, underscore)'
			}),
			z.string().min(1)
		)
		.default({})
		.meta(
			meta({
				label: 'Datasets',
				description:
					'Table name → dataset id. When empty, every queryable dataset in the workspace is exposed under a name derived from the dataset name.',
				category: 'visibility'
			})
		)
});

// Exactly one of token / keyfile.
export const couplerIoConnectionSchema = couplerIoBase.check(authGroupOneOfCheck(couplerIoBase));

export type CouplerIoConnection = z.infer<typeof couplerIoConnectionSchema>;

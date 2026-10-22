/**
 * Execution-layer Coupler.io credentials, resolved from connection.yaml.
 *
 * `tables` maps a table name to a dataset id; empty means auto-discover every
 * queryable dataset in the workspace.
 */
export type CouplerIoCredentials = {
	token: string;
	workspaceId: string;
	apiHost: string;
	queryHost: string;
	tables: Record<string, string>;
	/** Requests in flight at once; the rest queue. */
	maxConcurrentRequests: number;
};

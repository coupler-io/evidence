import { MotherDuckDialect } from './motherduck';

/**
 * Coupler.io dialect. POST /query runs DuckDB SQL, so this is the MotherDuck
 * (DuckDB) dialect under its own name — the name keys connector-specific
 * behaviour (describe probes, metadata loading).
 */
export class CouplerIoDialect extends MotherDuckDialect {
	override readonly name = 'coupler-io';
}

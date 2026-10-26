import { Database, Spanner } from '@google-cloud/spanner';

/**
 * The "default Spanner database" singleton.
 */
let database!: Database;

/**
 * Instantiates a database object using the configuration found in the environment variables.
 *
 * @returns The database.
 */
export function getDefaultSpannerDatabaseForCloudFunction(): Database {
  if (database) {
    return database;
  }

  const instanceName = process.env.SPANNER_INSTANCE ?? '';
  const databaseName = process.env.SPANNER_DATABASE ?? '';

  database = new Spanner().instance(instanceName).database(databaseName);

  return database;
}

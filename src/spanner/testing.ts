import type { NestJsModuleOverrider } from '@causa/runtime/nestjs/testing';
import { Database, Instance, Spanner } from '@google-cloud/spanner';
import * as uuid from 'uuid';

/**
 * Creates a new database.
 * This will destroy the existing database if it exists.
 *
 * @param options Options when creating the database.
 * @returns The database object.
 */
export async function createDatabase(
  options: {
    /**
     * The name of the database to create. If not specified, a random name will be generated.
     */
    name?: string;

    /**
     * If `sourceDatabaseName` is provided, its DDL will be copied into the new database, otherwise it will try to copy
     * the DDL from `process.env.SPANNER_DATABASE`.
     * If `null`, no schema will be set on the created database.
     */
    sourceDatabaseName?: string | null;

    /**
     * The instance on which the database should be created.
     * By default, a new Spanner client will be created using the `SPANNER_INSTANCE` environment variable.
     */
    instance?: Instance;

    /**
     * The Spanner client to use to create the database.
     * If `instance` is provided, this will be ignored.
     */
    spanner?: Spanner;
  } = {},
): Promise<Database> {
  const name = options.name ?? `test-${uuid.v4().slice(-10)}`;
  const spanner = options.spanner ?? new Spanner();
  const instance =
    options.instance ?? spanner.instance(process.env.SPANNER_INSTANCE ?? '');
  const sourceDatabaseName =
    options.sourceDatabaseName ?? process.env.SPANNER_DATABASE;

  const [databases] = await instance.getDatabases();
  const existingDatabase = databases.find(
    (d) => d.formattedName_.split('/').pop() === name,
  );
  await existingDatabase?.delete();

  const [database, operation] = await instance.createDatabase(name);
  await operation.promise();

  if (sourceDatabaseName) {
    const sourceDatabase = instance.database(sourceDatabaseName);
    const [schema] = await sourceDatabase.getSchema();
    await sourceDatabase.close();

    const [updateOperation] = await database.updateSchema(schema);
    await updateOperation.promise();
  }

  return database;
}

/**
 * Returns a {@link NestJsModuleOverrider} that overrides the {@link Database} provider with the provided database.
 *
 * @param database The temporary database to use.
 * @returns The {@link NestJsModuleOverrider} to override the {@link Database} provider.
 */
export function overrideDatabase(database: Database): NestJsModuleOverrider {
  return (builder) => builder.overrideProvider(Database).useValue(database);
}

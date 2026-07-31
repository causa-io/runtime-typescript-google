import type {
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import { Database, Instance, Spanner } from '@google-cloud/spanner';
import type { Type } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { SpannerEntityManager } from './entity-manager.js';

/**
 * Parameters for creating a new test database.
 */
type CreateDatabaseParameters = Pick<
  SpannerFixture,
  'name' | 'sourceDatabaseName' | 'spanner' | 'instance'
>;

/**
 * Sets default values for the database creation parameters.
 *
 * @param options The options for which defaults should be set where needed.
 * @returns The {@link CreateDatabaseParameters}.
 */
function makeDatabaseParameters(
  options: Partial<CreateDatabaseParameters>,
): CreateDatabaseParameters {
  const name = options.name ?? `test-${randomUUID().slice(-10)}`;
  const spanner = options.instance?.parent ?? options.spanner ?? new Spanner();
  const instance =
    options.instance ?? spanner.instance(process.env.SPANNER_INSTANCE ?? '');
  const sourceDatabaseName =
    options.sourceDatabaseName === null
      ? null
      : (options.sourceDatabaseName ?? process.env.SPANNER_DATABASE ?? null);

  return { name, spanner, instance, sourceDatabaseName };
}

/**
 * Creates a new database.
 * This will destroy the existing database if it exists.
 *
 * @param options Options when creating the database.
 * @returns The database object.
 */
export async function createDatabase(
  options: Partial<CreateDatabaseParameters> = {},
): Promise<Database> {
  const { name, sourceDatabaseName, instance, spanner } =
    makeDatabaseParameters(options);
  const [{ formattedName_: parent }] = await instance.get();
  const adminClient = spanner.getDatabaseAdminClient();

  const [databases] = await adminClient.listDatabases({ parent });
  const existingDatabase = databases.find(
    (d) => d.name?.split('/').pop() === name,
  );
  if (existingDatabase) {
    await adminClient.dropDatabase({ database: existingDatabase.name });
  }

  let extraStatements: string[] | undefined;
  if (sourceDatabaseName) {
    const sourceDatabase = instance.database(sourceDatabaseName);
    [extraStatements] = await sourceDatabase.getSchema();
    await sourceDatabase.close();
  }

  const [operation] = await adminClient.createDatabase({
    parent,
    createStatement: `CREATE DATABASE \`${name}\``,
    extraStatements,
  });
  await operation.promise();

  return instance.database(name);
}

/**
 * A {@link Fixture} that creates a temporary Spanner database and injects it into the NestJS application.
 * The specified tables will be cleared after each test.
 */
export class SpannerFixture implements Fixture {
  /**
   * The name of the temporary database.
   */
  readonly name: string;

  /**
   * If `sourceDatabaseName` is provided, its DDL will be copied into the new database, otherwise it will try to copy
   * the DDL from `process.env.SPANNER_DATABASE`.
   * If `null`, no schema will be set on the created database.
   */
  readonly sourceDatabaseName: string | null;

  /**
   * The Spanner client to use for tests.
   */
  readonly spanner: Spanner;

  /**
   * The Spanner instance to use for tests.
   * By default, a new Spanner client will be created using the `SPANNER_INSTANCE` environment variable.
   */
  readonly instance: Instance;

  /**
   * Types of entities (Spanner tables) to clear.
   */
  readonly types: Type[];

  /**
   * The {@link SpannerEntityManager} used to clear tables.
   * This is only set once the temporary database has been created.
   */
  private entityManager: SpannerEntityManager | undefined;

  /**
   * The promise creating the temporary test database.
   */
  private databasePromise: Promise<Database> | undefined;

  constructor(
    options: Partial<CreateDatabaseParameters> &
      Partial<Pick<SpannerFixture, 'types'>> = {},
  ) {
    const { name, sourceDatabaseName, spanner, instance } =
      makeDatabaseParameters(options);
    this.name = name;
    this.sourceDatabaseName = sourceDatabaseName;
    this.spanner = spanner;
    this.instance = instance;
    this.types = options.types ?? [];
  }

  async init(): Promise<NestJsModuleOverrider> {
    // This ensures that the temporary database is only created if the application provides a `Database` (uses Spanner).
    return (builder) =>
      builder
        .overrideProvider(Database)
        .useFactory({ factory: () => this.getOrCreateDatabase() });
  }

  /**
   * Creates the temporary test database if it does not exist yet.
   *
   * @returns The temporary test {@link Database}.
   */
  private getOrCreateDatabase(): Promise<Database> {
    this.databasePromise ??= createDatabase(this).then((database) => {
      this.entityManager = new SpannerEntityManager(database);
      return database;
    });

    return this.databasePromise;
  }

  async clear(): Promise<void> {
    await this.entityManager?.transaction(async (transaction) => {
      for (const entity of this.types) {
        await this.entityManager?.clear(entity, { transaction });
      }
    });
  }

  async delete(): Promise<void> {
    const database = await this.databasePromise?.catch(() => undefined);
    await database?.delete();

    this.spanner.close();
  }
}

import type {
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import { Database, Instance, Spanner } from '@google-cloud/spanner';
import type { Type } from '@nestjs/common';
import * as uuid from 'uuid';
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
  const name = options.name ?? `test-${uuid.v4().slice(-10)}`;
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
  const { name, sourceDatabaseName, instance } =
    makeDatabaseParameters(options);

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
   */
  private entityManager!: SpannerEntityManager;

  /**
   * The temporary test database created by this fixture.
   */
  private database!: Database;

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
    this.database = await createDatabase(this);

    this.entityManager = new SpannerEntityManager(this.database);

    return (builder) =>
      builder.overrideProvider(Database).useValue(this.database);
  }

  async clear(): Promise<void> {
    await this.entityManager.transaction(async (transaction) => {
      for (const entity of this.types) {
        await this.entityManager.clear(entity, { transaction });
      }
    });
  }

  async delete(): Promise<void> {
    await this.database.delete();

    this.spanner.close();
  }
}

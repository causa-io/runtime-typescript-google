import { Database } from '@google-cloud/spanner';
import { SpannerColumn } from './column.decorator.js';
import { SpannerEntityManager } from './entity-manager.js';
import { SpannerTable } from './table.decorator.js';
import { createDatabase } from './testing.js';

@SpannerTable({ name: 'MyEntity', primaryKey: ['id'] })
export class SomeEntity {
  constructor(data: Partial<SomeEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn()
  value!: string;
}

@SpannerTable({ primaryKey: ['id'] })
export class IntEntity {
  constructor(data: Partial<IntEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn({ isBigInt: true })
  value!: bigint;
}

@SpannerTable({ primaryKey: ['id'] })
export class IndexedEntity {
  constructor(data: Partial<IndexedEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn({ isInt: true })
  value!: number;

  @SpannerColumn()
  otherValue!: string;

  @SpannerColumn()
  notStored!: string | null;

  static readonly ByValue = 'IndexedEntitiesByValue';
}

@SpannerTable({ primaryKey: ['id'] })
export class ParentEntity {
  constructor(data: Partial<ParentEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;
}

@SpannerTable({ primaryKey: ['id'] })
export class SoftDeleteEntity {
  constructor(data: Partial<SoftDeleteEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn()
  id!: string;

  @SpannerColumn({ softDelete: true })
  deletedAt!: Date | null;
}

@SpannerTable({ primaryKey: ['address.city', 'address.zip', 'id'] })
export class NestedKeyEntity {
  constructor(data: Partial<NestedKeyEntity> = {}) {
    Object.assign(this, data);
  }

  @SpannerColumn({ isInt: true })
  id!: number;

  @SpannerColumn({ isJson: true })
  address!: {
    city: string;
    zip: string;
  };

  @SpannerColumn()
  value!: string;
}

export const TEST_SCHEMA = [
  `CREATE TABLE MyEntity (
    id STRING(MAX) NOT NULL,
    value STRING(MAX) NOT NULL
  ) PRIMARY KEY (id)`,
  `CREATE TABLE MyInterleavedEntity (
    id STRING(MAX) NOT NULL,
  ) PRIMARY KEY (id),
  INTERLEAVE IN PARENT MyEntity ON DELETE CASCADE`,
  `CREATE TABLE IntEntity (
    id STRING(MAX) NOT NULL,
    value INT64 NOT NULL
  ) PRIMARY KEY (id)`,
  `CREATE TABLE IndexedEntity (
    id STRING(MAX) NOT NULL,
    value INT64 NOT NULL,
    otherValue STRING(MAX) NOT NULL,
    notStored STRING(MAX)
  ) PRIMARY KEY (id)`,
  `CREATE INDEX IndexedEntitiesByValue ON IndexedEntity(value) STORING (otherValue)`,
  `CREATE TABLE ParentEntity (
    id STRING(MAX) NOT NULL,
  ) PRIMARY KEY (id)`,
  `CREATE TABLE SoftDeleteEntity (
    id STRING(MAX) NOT NULL,
    deletedAt TIMESTAMP
  ) PRIMARY KEY (id)`,
  `CREATE TABLE NestedKeyEntity (
    id INT64 NOT NULL,
    address JSON NOT NULL,
    value STRING(MAX) NOT NULL,
    addressCity STRING(MAX) AS (JSON_VALUE(address, '$.city')) STORED,
    addressZip STRING(MAX) AS (JSON_VALUE(address, '$.zip')) STORED,
  ) PRIMARY KEY (addressCity, addressZip, id)`,
  `CREATE INDEX NestedKeyEntityByZip ON NestedKeyEntity(addressZip) STORING (address)`,
];

/**
 * Helper function to setup a test database with schema and manager.
 * Returns cleanup function to be called in afterAll.
 */
export async function setupTestDatabase(): Promise<{
  database: Database;
  cleanup: () => Promise<void>;
}> {
  const database = await createDatabase();
  const [operation] = await database.updateSchema(TEST_SCHEMA);
  await operation.promise();

  return {
    database,
    cleanup: async () => {
      await database.delete();
    },
  };
}

/**
 * Helper function to clear all test entities from the database.
 */
export async function clearAllTestEntities(
  manager: SpannerEntityManager,
): Promise<void> {
  await manager.transaction(async (transaction) => {
    await manager.clear(SomeEntity, { transaction });
    await manager.clear(IntEntity, { transaction });
    await manager.clear(IndexedEntity, { transaction });
    await manager.clear(ParentEntity, { transaction });
    await manager.clear(SoftDeleteEntity, { transaction });
    await manager.clear(NestedKeyEntity, { transaction });
  });
}

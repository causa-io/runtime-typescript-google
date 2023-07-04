import { EntityNotFoundError } from '@causa/runtime';
import { Database, Snapshot, Transaction } from '@google-cloud/spanner';
import { Int, Type } from '@google-cloud/spanner/build/src/codec.js';
import { TimestampBounds } from '@google-cloud/spanner/build/src/transaction.js';
import {
  instanceToSpannerObject,
  spannerObjectToInstance,
} from './conversion.js';
import { convertSpannerToEntityError } from './error-converter.js';
import {
  EntityMissingPrimaryKeyError,
  TransactionFinishedError,
} from './errors.js';
import { SpannerTableCache } from './table-cache.js';
import { RecursivePartialEntity } from './types.js';

/**
 * Any Spanner transaction that can be used for reading.
 */
export type SpannerReadOnlyTransaction = Snapshot | Transaction;

/**
 * A key for a Spanner row.
 */
export type SpannerKey = (string | null)[];

/**
 * Base options for all write operations.
 */
type WriteOperationOptions = {
  /**
   * The {@link Transaction} to use.
   */
  transaction?: Transaction;
};

/**
 * Base options for all read operations.
 */
type ReadOperationOptions = {
  /**
   * The {@link Transaction} or {@link Snapshot} to use.
   */
  transaction?: SpannerReadOnlyTransaction;
};

/**
 * Options for {@link SpannerEntityManager.snapshot}.
 */
type SnapshotOptions = {
  /**
   * Sets how the timestamp will be selected when creating the snapshot.
   */
  timestampBounds?: TimestampBounds;
};

/**
 * A function that can be passed to the {@link SpannerEntityManager.snapshot} method.
 */
export type SnapshotFunction<T> = (snapshot: Snapshot) => Promise<T>;

/**
 * A SQL statement run using {@link SpannerEntityManager.query}.
 */
export type SqlStatement = {
  /**
   * The SQL statement to run.
   */
  sql: string;

  /**
   * The values for the parameters referenced in the statement.
   */
  params?: Record<string, any>;

  /**
   * The types of the parameters in the statement.
   */
  types?: Record<string, Type>;
};

/**
 * Options for {@link SpannerEntityManager.query}.
 */
export type QueryOptions<T> = ReadOperationOptions & {
  /**
   * The type of entity to return in the list of results.
   */
  entityType?: { new (): T };
};

/**
 * Options when reading entities.
 */
type FindOptions = ReadOperationOptions & {
  /**
   * The index to use to look up the entity.
   */
  index?: string;

  /**
   * The columns to fetch. If not provided, all columns will be fetched.
   */
  columns?: string[];
};

/**
 * A class that manages access to entities stored in a Cloud Spanner database.
 * Entities are defined by classes decorated with the `SpannerTable` and `SpannerColumn` decorators.
 */
export class SpannerEntityManager {
  /**
   * A cache storing the `SpannerTableMetadata` for each entity type (class).
   */
  protected readonly tableCache = new SpannerTableCache();

  /**
   * Creates a new {@link SpannerEntityManager}.
   *
   * @param database The {@link Database} to use to connect to Spanner.
   */
  constructor(readonly database: Database) {}

  /**
   * Returns the primary key of the given entity.
   * The primary key is the ordered list of values of the columns that make up the key.
   *
   * @param entity The entity for which to get the primary key.
   * @param entityType The type of the entity. If not provided, the type will be inferred from the entity (using its
   *   constructor).
   * @returns The primary key of the entity.
   */
  getPrimaryKey<T>(
    entity: T | RecursivePartialEntity<T>,
    entityType?: { new (): T },
  ): SpannerKey {
    entityType ??= (entity as any).constructor as { new (): T };
    const obj = instanceToSpannerObject(entity, entityType);
    return this.getPrimaryKeyForSpannerObject(obj, entityType);
  }

  /**
   * Returns the primary key of the given Spanner object, assumed to be an entity of the given type.
   *
   * @param obj The Spanner object for which to get the primary key.
   * @param entityType The type of the entity.
   * @returns The primary key of the entity.
   */
  protected getPrimaryKeyForSpannerObject(
    obj: Record<string, any>,
    entityType: { new (): any },
  ): SpannerKey {
    const { primaryKeyColumns } = this.tableCache.getMetadata(entityType);

    return primaryKeyColumns.map((c) => {
      const value = obj[c];

      if (value === undefined) {
        throw new EntityMissingPrimaryKeyError();
      }

      if (value instanceof Int) {
        return value.value;
      }

      if (value instanceof Date) {
        return value.toJSON();
      }

      return value;
    });
  }

  /**
   * Fetches a single row from the database using its key (either primary or for a secondary index).
   * If a secondary index is specified but not the columns to fetch, all the columns will be returned by performing an
   * additional read. To avoid this, specify the columns to fetch. Those columns should be part of the primary key, the
   * indexed columns, or the stored columns of the index.
   *
   * @param entityType The type of entity to fetch. Used to determine the table name and columns to fetch.
   * @param key The key of the entity to fetch. This can be the primary key, or the key of a secondary index.
   * @param options Options when reading the entity (e.g. the index to use and the columns to fetch).
   * @returns The row returned by Spanner, or `undefined` if it was not found.
   */
  protected async findRowByKey(
    entityType: { new (): any },
    key: SpannerKey | SpannerKey[number],
    options: FindOptions = {},
  ): Promise<Record<string, any> | undefined> {
    if (!Array.isArray(key)) {
      key = [key];
    }

    const {
      tableName,
      columns: allColumns,
      primaryKeyColumns,
    } = this.tableCache.getMetadata(entityType);
    const columns =
      options.columns ?? (options.index ? primaryKeyColumns : allColumns);

    return await this.runInExistingOrNewReadOnlyTransaction(
      options.transaction,
      async (transaction) => {
        const [rows] = await transaction.read(tableName, {
          keys: [key as any],
          columns,
          limit: 1,
          json: true,
          jsonOptions: { wrapNumbers: true },
          index: options.index,
        });
        const row = rows[0];

        if (row && options.index && !options.columns) {
          const primaryKey = this.getPrimaryKeyForSpannerObject(
            row,
            entityType,
          );
          return await this.findRowByKey(entityType, primaryKey, {
            transaction,
          });
        }

        return row;
      },
    );
  }

  /**
   * Fetches a single entity from the database using its key (either primary or for a secondary index).
   * If a secondary index is specified but not the columns to fetch, all the columns will be returned by performing an
   * additional read. To avoid this, specify the columns to fetch. Those columns should be part of the primary key, the
   * indexed columns, or the stored columns of the index.
   *
   * @param entityType The type of entity to return.
   * @param key The key of the entity to return. This can be the primary key, or the key of a secondary index.
   * @param options Options when reading the entity (e.g. the index to use and the columns to fetch).
   * @returns The entity, or `undefined` if it was not found.
   */
  async findOneByKey<T>(
    entityType: { new (): T },
    key: SpannerKey | SpannerKey[number],
    options: FindOptions = {},
  ): Promise<T | undefined> {
    const row = await this.findRowByKey(entityType, key, options);
    if (!row) {
      return undefined;
    }

    return spannerObjectToInstance(row, entityType);
  }

  /**
   * Fetches a single entity from the database using its key (either primary or for a secondary index).
   * See {@link SpannerEntityManager.findOneByKey} for more details.
   * If the entity is not found, an {@link EntityNotFoundError} is thrown.
   *
   * @param entityType The type of entity to return.
   * @param key The key of the entity to return. This can be the primary key, or the key of a secondary index.
   * @param options Options when reading the entity (e.g. the index to use and the columns to fetch).
   * @returns The fetched entity.
   */
  async findOneByKeyOrFail<T>(
    entityType: { new (): T },
    key: SpannerKey | SpannerKey[number],
    options: FindOptions = {},
  ): Promise<T> {
    const entity = await this.findOneByKey(entityType, key, options);
    if (!entity) {
      throw new EntityNotFoundError(entityType, key);
    }

    return entity;
  }

  /**
   * Runs the provided function in a (read write) {@link Transaction}.
   * The function itself should not commit or rollback the transaction.
   * If the function throws an error, the transaction will be rolled back.
   *
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  async transaction<T>(
    runFn: (transaction: Transaction) => Promise<T>,
  ): Promise<T> {
    try {
      return await this.database.runTransactionAsync(async (transaction) => {
        try {
          const result = await runFn(transaction);

          if (transaction.ended) {
            throw new TransactionFinishedError();
          }

          await transaction.commit();

          return result;
        } catch (error) {
          if (!transaction.ended) {
            if (transaction.id) {
              await transaction.rollback();
            } else {
              transaction.end();
            }
          }

          throw error;
        }
      });
    } catch (error) {
      throw convertSpannerToEntityError(error) ?? error;
    }
  }

  /**
   * Runs the provided function in a read-only transaction ({@link Snapshot}).
   * The snapshot will be automatically released when the function returns.
   *
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  snapshot<T>(runFn: SnapshotFunction<T>): Promise<T>;
  /**
   * Runs the provided function in a read-only transaction ({@link Snapshot}).
   * The snapshot will be automatically released when the function returns.
   *
   * @param options The options to use when creating the snapshot.
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  snapshot<T>(options: SnapshotOptions, runFn: SnapshotFunction<T>): Promise<T>;
  async snapshot<T>(
    optionsOrRunFn: SnapshotOptions | SnapshotFunction<T>,
    runFn?: (snapshot: Snapshot) => Promise<T>,
  ): Promise<T> {
    const snapshotFn =
      typeof optionsOrRunFn === 'function'
        ? optionsOrRunFn
        : (runFn as SnapshotFunction<T>);
    const options: SnapshotOptions =
      typeof optionsOrRunFn === 'object' ? optionsOrRunFn : {};

    let snapshot: Snapshot | undefined;
    try {
      [snapshot] = await this.database.getSnapshot(options.timestampBounds);
      return await snapshotFn(snapshot);
    } catch (error) {
      throw convertSpannerToEntityError(error) ?? error;
    } finally {
      snapshot?.end();
    }
  }

  /**
   * Deletes all rows from the table corresponding to the given entity type.
   *
   * @param entityType The type of entity for which the table should be cleared.
   * @param options The options to use when running the operation.
   */
  async clear(
    entityType: { new (): any },
    options: WriteOperationOptions = {},
  ): Promise<void> {
    const { quotedTableName } = this.tableCache.getMetadata(entityType);

    await this.runInExistingOrNewTransaction(
      options.transaction,
      (transaction) =>
        transaction.runUpdate({
          sql: `DELETE FROM ${quotedTableName} WHERE TRUE`,
        }),
    );
  }

  /**
   * Runs the given SQL statement in the database.
   * By default, the statement is run in a read-only transaction ({@link Snapshot}). To perform a write operation, pass
   * a {@link Transaction} in the options.
   *
   * @param options Options for the operation.
   * @param statement The SQL statement to run.
   * @returns The rows returned by the query. If {@link QueryOptions.entityType} is set, the rows are converted to
   *   instances of that class.
   */
  query<T>(options: QueryOptions<T>, statement: SqlStatement): Promise<T[]>;
  /**
   * Runs the given SQL statement in the database.
   * The statement is run in a read-only transaction ({@link Snapshot}).
   *
   * @param statement The SQL statement to run.
   * @returns The rows returned by the query.
   */
  query<T>(statement: SqlStatement): Promise<T[]>;
  async query<T>(
    optionsOrStatement: QueryOptions<T> | SqlStatement,
    statement?: SqlStatement,
  ): Promise<T[]> {
    const options: QueryOptions<T> = statement
      ? (optionsOrStatement as QueryOptions<T>)
      : {};
    const sqlStatement = statement ?? (optionsOrStatement as SqlStatement);
    const { entityType } = options;

    return await this.runInExistingOrNewReadOnlyTransaction(
      options.transaction,
      async (transaction) => {
        const [rows] = await transaction.run({
          ...sqlStatement,
          json: true,
          jsonOptions: { wrapNumbers: entityType != null },
        });

        if (entityType) {
          return rows.map((row) => spannerObjectToInstance(row, entityType));
        }

        return rows as T[];
      },
    );
  }

  /**
   * Inserts the given entity into the database.
   * If the entity already exists, an error will be thrown.
   *
   * @param entity The entity to insert.
   * @param options Options for the operation.
   */
  async insert(
    entity: any,
    options: WriteOperationOptions = {},
  ): Promise<void> {
    const entityType = entity.constructor;
    const { tableName } = this.tableCache.getMetadata(entityType);
    const obj = instanceToSpannerObject(entity, entityType);

    await this.runInExistingOrNewTransaction(
      options.transaction,
      async (transaction) => transaction.insert(tableName, obj),
    );
  }

  /**
   * Replaces the given entity in the database.
   * If the entity already exists, all columns are overwritten, even if they are not present in the entity (in the case
   * of nullable columns).
   *
   * @param entity The entity to write.
   * @param options Options for the operation.
   */
  async replace(
    entity: any,
    options: WriteOperationOptions = {},
  ): Promise<void> {
    const entityType = entity.constructor;
    const { tableName } = this.tableCache.getMetadata(entityType);
    const obj = instanceToSpannerObject(entity, entityType);

    await this.runInExistingOrNewTransaction(
      options.transaction,
      async (transaction) => transaction.replace(tableName, obj),
    );
  }

  /**
   * Deletes the given entity from the database, or throws an error if it does not exist.
   *
   * @param entityType The type of entity to delete.
   * @param key The primary key of the entity to delete.
   * @param options Options for the operation.
   * @returns The deleted entity.
   */
  async delete<T>(
    entityType: { new (): T },
    key: SpannerKey | SpannerKey[number],
    options: WriteOperationOptions & {
      /**
       * A function that will be called with the entity before it is deleted.
       * This function can throw an error to prevent the deletion.
       */
      validateFn?: (entity: T) => void;
    } = {},
  ): Promise<T> {
    if (!Array.isArray(key)) {
      key = [key];
    }
    const { tableName } = this.tableCache.getMetadata(entityType);

    return await this.runInExistingOrNewTransaction(
      options.transaction,
      async (transaction) => {
        const existingEntity = await this.findOneByKeyOrFail(entityType, key, {
          transaction,
        });

        if (options.validateFn) {
          options.validateFn(existingEntity);
        }

        transaction.deleteRows(tableName, [key as any]);

        return existingEntity;
      },
    );
  }

  /**
   * Runs the given "read-write" function on a transaction. If a transaction is not passed, a new {@link Transaction} is
   * created instead.
   *
   * @param transaction The transaction to use. If `undefined`, a new transaction is created.
   * @param fn The function to run on the transaction.
   * @returns The result of the function.
   */
  async runInExistingOrNewTransaction<T>(
    transaction: Transaction | undefined,
    fn: (transaction: Transaction) => Promise<T>,
  ) {
    if (transaction) {
      try {
        return await fn(transaction);
      } catch (error) {
        throw convertSpannerToEntityError(error) ?? error;
      }
    }

    return this.transaction(fn);
  }

  /**
   * Runs the given "read-only" function on a transaction. If a transaction is not passed, a new {@link Snapshot} is
   * created instead.
   *
   * @param transaction The transaction to use. If `undefined`, a new {@link Snapshot} is created.
   * @param fn The function to run on the transaction.
   * @returns The result of the function.
   */
  async runInExistingOrNewReadOnlyTransaction<T>(
    transaction: SpannerReadOnlyTransaction | undefined,
    fn: (transaction: SpannerReadOnlyTransaction) => Promise<T>,
  ) {
    if (transaction) {
      try {
        return await fn(transaction);
      } catch (error) {
        throw convertSpannerToEntityError(error) ?? error;
      }
    }

    return this.snapshot(fn);
  }
}

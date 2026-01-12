import { EntityNotFoundError } from '@causa/runtime';
import { Database, Snapshot } from '@google-cloud/spanner';
import type {
  ExecuteSqlRequest,
  TimestampBounds,
} from '@google-cloud/spanner/build/src/transaction.js';
import { Injectable, type Type } from '@nestjs/common';
import {
  copyInstanceWithMissingColumnsToNull,
  instanceToSpannerObject,
  spannerObjectToInstance,
  updateInstanceByColumn,
} from './conversion.js';
import { convertSpannerToEntityError } from './error-converter.js';
import { InvalidArgumentError, TransactionFinishedError } from './errors.js';
import { SpannerTableCache } from './table-cache.js';
import type {
  SpannerKey,
  SpannerReadOnlyTransaction,
  SpannerReadOnlyTransactionOption,
  SpannerReadWriteTransaction,
  SpannerReadWriteTransactionOption,
  SqlParamType,
  SqlStatement,
} from './types.js';

/**
 * Options for {@link SpannerEntityManager.snapshot}.
 */
type SnapshotOptions =
  | SpannerReadOnlyTransactionOption
  | {
      /**
       * Sets how the timestamp will be selected when creating the snapshot.
       */
      timestampBounds?: TimestampBounds;
    };

/**
 * A function that can be passed to the {@link SpannerEntityManager.snapshot} method.
 */
export type SnapshotFunction<T> = (
  snapshot: SpannerReadOnlyTransaction,
) => Promise<T>;

/**
 * A function that can be passed to the {@link SpannerEntityManager.transaction} method.
 */
export type SpannerTransactionFunction<T> = (
  transaction: SpannerReadWriteTransaction,
) => Promise<T>;

/**
 * Options for {@link SpannerEntityManager.query}.
 */
export type QueryOptions<T> = SpannerReadOnlyTransactionOption &
  Pick<ExecuteSqlRequest, 'requestOptions'> & {
    /**
     * The type of entity to return in the list of results.
     */
    entityType?: Type<T>;
  };

/**
 * Options when reading entities.
 */
type FindOptions = SpannerReadOnlyTransactionOption &
  Pick<ExecuteSqlRequest, 'requestOptions'> & {
    /**
     * The index to use to look up the entity.
     */
    index?: string;

    /**
     * The columns to fetch. If not provided, all columns will be fetched.
     */
    columns?: string[];

    /**
     * If `true`, soft-deleted entities will be included in the results.
     * Defaults to `false`.
     */
    includeSoftDeletes?: boolean;
  };

/**
 * A class that manages access to entities stored in a Cloud Spanner database.
 * Entities are defined by classes decorated with the `SpannerTable` and `SpannerColumn` decorators.
 */
@Injectable()
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
  getPrimaryKey<T>(entity: T | Partial<T>, entityType?: Type<T>): SpannerKey {
    entityType ??= (entity as any).constructor as Type<T>;
    const { primaryKeyGetter } = this.tableCache.getMetadata(entityType);
    return primaryKeyGetter(entity);
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
    entityType: Type,
  ): SpannerKey {
    const entity = spannerObjectToInstance(obj, entityType);
    return this.getPrimaryKey(entity, entityType);
  }

  /**
   * Returns the (quoted) name of the table for the given entity type.
   *
   * @deprecated Use {@link sqlTable} instead.
   *
   * @param entityTypeOrTable The type of entity, or the unquoted table name.
   * @param options Options when constructing the table name (e.g. the index to use).
   * @returns The name of the table, quoted with backticks.
   */
  sqlTableName(
    entityTypeOrTable: Type | string,
    options: {
      /**
       * Sets a table hint to indicate which index to use when querying the table.
       * The value will be quoted with backticks.
       */
      index?: string;

      /**
       * Sets a table hint to disable the check that prevents queries from using null-filtered indexes.
       * This is useful when using the emulator, which does not support null-filtered indexes.
       */
      disableQueryNullFilteredIndexEmulatorCheck?: boolean;
    } = {},
  ): string {
    return this.sqlTable(entityTypeOrTable, options);
  }

  /**
   * Returns the (quoted) name of the table for the given entity type.
   *
   * @param entityTypeOrTable The type of entity, or the unquoted table name.
   * @param options Options when constructing the table name (e.g. the index to use).
   * @returns The name of the table, quoted with backticks.
   */
  sqlTable(
    entityTypeOrTable: Type | string,
    options: {
      /**
       * Sets a table hint to indicate which index to use when querying the table.
       * The value will be quoted with backticks.
       */
      index?: string;

      /**
       * Sets a table hint to disable the check that prevents queries from using null-filtered indexes.
       * This is useful when using the emulator, which does not support null-filtered indexes.
       */
      disableQueryNullFilteredIndexEmulatorCheck?: boolean;
    } = {},
  ): string {
    const tableHints: Record<string, string> = {};
    if (options.index) {
      tableHints.FORCE_INDEX = `\`${options.index}\``;
    }
    if (options.disableQueryNullFilteredIndexEmulatorCheck) {
      tableHints['spanner_emulator.disable_query_null_filtered_index_check'] =
        'true';
    }
    const tableHintsString = Object.entries(tableHints)
      .map(([k, v]) => `${k}=${v}`)
      .join(',');

    const tableName =
      typeof entityTypeOrTable === 'string'
        ? entityTypeOrTable
        : this.tableCache.getMetadata(entityTypeOrTable).tableName;
    const quotedTableName = `\`${tableName}\``;

    return tableHintsString.length > 0
      ? `${quotedTableName}@{${tableHintsString}}`
      : quotedTableName;
  }

  /**
   * Returns the (quoted) list of columns for the given entity type or list of columns.
   *
   * If a type is provided, all columns are included.
   * If a list of columns is provided, they are assumed to be unquoted.
   *
   * @param entityTypeOrColumns The type of entity, or the unquoted list of columns.
   * @returns The list of columns, quoted with backticks and joined.
   */
  sqlColumns<T = unknown>(
    entityTypeOrColumns: Type<T> | string[],
    options: {
      /**
       * If `entityTypeOrColumns` is a type, only the columns for the given properties will be included.
       */
      forProperties?: T extends object ? (keyof T & string)[] : never;

      /**
       * The alias with which to prefix each column.
       */
      alias?: string;
    } = {},
  ): string {
    let columns: string[];

    if (Array.isArray(entityTypeOrColumns)) {
      columns = entityTypeOrColumns;
    } else {
      const { columnNames } = this.tableCache.getMetadata(entityTypeOrColumns);
      columns = options.forProperties
        ? options.forProperties.map((p) => columnNames[p])
        : Object.values(columnNames);
    }

    columns = columns.map((c) => `\`${c}\``);
    if (options.alias) {
      columns = columns.map((c) => `\`${options.alias}\`.${c}`);
    }

    return columns.join(', ');
  }

  /**
   * Fetches a single row from the database using its key (either primary or for a secondary index).
   *
   * If a secondary index is specified but not the columns to fetch, all the columns will be returned by performing an
   * additional read. To avoid this, specify the columns to fetch. Those columns should be part of the primary key, the
   * indexed columns, or the stored columns of the index.
   *
   * By default, soft-deleted entities will not be returned. To include them, set `includeSoftDeletes` to `true`. This
   * also means that the soft delete column should be included in the columns to fetch unless `includeSoftDeletes` is
   * set to `true`.
   *
   * @param entityType The type of entity to fetch. Used to determine the table name and columns to fetch.
   * @param key The key of the entity to fetch. This can be the primary key, or the key of a secondary index.
   * @param options Options when reading the entity (e.g. the index to use and the columns to fetch).
   * @returns The row returned by Spanner, or `undefined` if it was not found.
   */
  protected async findRowByKey(
    entityType: Type,
    key: SpannerKey | SpannerKey[number],
    options: FindOptions = {},
  ): Promise<Record<string, any> | undefined> {
    if (!Array.isArray(key)) {
      key = [key];
    }

    const { tableName, columnNames, primaryKeyColumns, softDeleteColumn } =
      this.tableCache.getMetadata(entityType);
    const columns =
      options.columns ??
      (options.index ? primaryKeyColumns : Object.values(columnNames));

    return await this.snapshot(
      { transaction: options.transaction },
      async (transaction) => {
        const [rows] = await transaction.read(tableName, {
          keys: [key as any],
          columns,
          limit: 1,
          json: true,
          jsonOptions: { wrapNumbers: true },
          index: options.index,
          requestOptions: options.requestOptions,
        });
        const row: Record<string, any> | undefined = rows[0];

        if (row && options.index && !options.columns) {
          const primaryKey = this.getPrimaryKeyForSpannerObject(
            row,
            entityType,
          );
          return await this.findRowByKey(entityType, primaryKey, {
            transaction,
          });
        }

        if (row && softDeleteColumn && !options.includeSoftDeletes) {
          const value = row[softDeleteColumn];
          if (value) {
            return undefined;
          } else if (value === undefined) {
            throw new InvalidArgumentError(
              `The soft delete column should be included in the columns to fetch, or 'includeSoftDeletes' should be set to true.`,
            );
          }
        }

        return row;
      },
    );
  }

  /**
   * Fetches a single entity from the database using its key (either primary or for a secondary index).
   *
   * If a secondary index is specified but not the columns to fetch, all the columns will be returned by performing an
   * additional read. To avoid this, specify the columns to fetch. Those columns should be part of the primary key, the
   * indexed columns, or the stored columns of the index.
   *
   * By default, soft-deleted entities will not be returned. To include them, set `includeSoftDeletes` to `true`. This
   * also means that the soft delete column should be included in the columns to fetch unless `includeSoftDeletes` is
   * set to `true`.
   *
   * @param entityType The type of entity to return.
   * @param key The key of the entity to return. This can be the primary key, or the key of a secondary index.
   * @param options Options when reading the entity (e.g. the index to use and the columns to fetch).
   * @returns The entity, or `undefined` if it was not found.
   */
  async findOneByKey<T>(
    entityType: Type<T>,
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
    entityType: Type<T>,
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
   * Runs the provided function in a (read write) {@link SpannerReadWriteTransaction}.
   * The function itself should not commit or rollback the transaction.
   * If the function throws an error, the transaction will be rolled back.
   *
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  transaction<T>(runFn: SpannerTransactionFunction<T>): Promise<T>;
  /**
   * Runs the provided function in a (read write) {@link SpannerReadWriteTransaction}.
   * The function itself should not commit or rollback the transaction.
   * If the function throws an error, the transaction will be rolled back.
   *
   * @param options The options to use when creating the transaction.
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  transaction<T>(
    options: SpannerReadWriteTransactionOption,
    runFn: SpannerTransactionFunction<T>,
  ): Promise<T>;
  async transaction<T>(
    optionsOrRunFn:
      | SpannerReadWriteTransactionOption
      | SpannerTransactionFunction<T>,
    runFn?: SpannerTransactionFunction<T>,
  ): Promise<T> {
    const options = runFn
      ? (optionsOrRunFn as SpannerReadWriteTransactionOption)
      : {};
    runFn ??= optionsOrRunFn as SpannerTransactionFunction<T>;

    try {
      if (options.transaction) {
        return await runFn(options.transaction);
      }

      return await this.database.runTransactionAsync(
        { requestOptions: { transactionTag: options.tag } },
        async (transaction) => {
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
        },
      );
    } catch (error) {
      throw convertSpannerToEntityError(error) ?? error;
    }
  }

  /**
   * Runs the provided function in a {@link SpannerReadOnlyTransaction}.
   * The snapshot will be automatically released when the function returns.
   *
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  snapshot<T>(runFn: SnapshotFunction<T>): Promise<T>;
  /**
   * Runs the provided function in a {@link SpannerReadOnlyTransaction}.
   * The snapshot will be automatically released when the function returns.
   *
   * @param options The options to use when creating the snapshot.
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  snapshot<T>(options: SnapshotOptions, runFn: SnapshotFunction<T>): Promise<T>;
  async snapshot<T>(
    optionsOrRunFn: SnapshotOptions | SnapshotFunction<T>,
    runFn?: SnapshotFunction<T>,
  ): Promise<T> {
    const options = runFn ? (optionsOrRunFn as SnapshotOptions) : {};
    runFn ??= optionsOrRunFn as SnapshotFunction<T>;

    let snapshot: Snapshot | undefined;
    try {
      let transaction: SpannerReadOnlyTransaction | undefined;
      if ('transaction' in options) {
        transaction = options.transaction;
      }

      if (!transaction) {
        [snapshot] = await this.database.getSnapshot(
          'timestampBounds' in options ? options.timestampBounds : undefined,
        );
        transaction = snapshot;
      }

      return await runFn(transaction);
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
    entityType: Type,
    options: SpannerReadWriteTransactionOption = {},
  ): Promise<void> {
    const { tableName } = this.tableCache.getMetadata(entityType);

    await this.transaction(options, (transaction) =>
      transaction.runUpdate(`DELETE FROM \`${tableName}\` WHERE TRUE`),
    );
  }

  /**
   * Runs the given SQL statement in the database.
   * By default, the statement is run in a {@link SpannerReadOnlyTransaction}. To perform a write operation, pass a
   * {@link SpannerReadWriteTransaction} in the options.
   *
   * @param options Options for the operation.
   * @param statement The SQL statement to run.
   * @returns The rows returned by the query. If {@link QueryOptions.entityType} is set, the rows are converted to
   *   instances of that class.
   */
  query<T>(options: QueryOptions<T>, statement: SqlStatement): Promise<T[]>;
  /**
   * Runs the given SQL statement in the database.
   * The statement is run in a {@link SpannerReadOnlyTransaction}.
   *
   * @param statement The SQL statement to run.
   * @returns The rows returned by the query.
   */
  query<T>(statement: SqlStatement): Promise<T[]>;
  async query<T>(
    optionsOrStatement: QueryOptions<T> | SqlStatement,
    statement?: SqlStatement,
  ): Promise<T[]> {
    const results: T[] = [];

    for await (const row of this.queryStream<T>(
      optionsOrStatement as any,
      statement as any,
    )) {
      results.push(row);
    }

    return results;
  }

  /**
   * Runs the given SQL statement in the database, returning an async iterable of the results.
   * By default, the statement is run in a {@link SpannerReadOnlyTransaction}. To perform a write operation, pass a
   * {@link SpannerReadWriteTransaction} in the options.
   *
   * @param options Options for the operation.
   * @param statement The SQL statement to run.
   * @returns An async iterable that yields the rows returned by the query.
   *   If {@link QueryOptions.entityType} is set, the rows are converted to instances of that class.
   */
  queryStream<T>(
    options: QueryOptions<T>,
    statement: SqlStatement,
  ): AsyncIterable<T>;
  /**
   * Runs the given SQL statement in the database, returning an async iterable of the results.
   * The statement is run in a {@link SpannerReadOnlyTransaction}.
   *
   * @param statement The SQL statement to run.
   * @returns An async iterable that yields the rows returned by the query.
   */
  queryStream<T>(statement: SqlStatement): AsyncIterable<T>;
  async *queryStream<T>(
    optionsOrStatement: QueryOptions<T> | SqlStatement,
    statement?: SqlStatement,
  ): AsyncIterable<T> {
    const options: QueryOptions<T> = statement
      ? (optionsOrStatement as QueryOptions<T>)
      : {};
    const sqlStatement = statement ?? (optionsOrStatement as SqlStatement);
    const { entityType, requestOptions, transaction } = options;

    let snapshot: Snapshot | undefined;
    try {
      let txn = transaction;
      if (!txn) {
        [snapshot] = await this.database.getSnapshot();
        txn = snapshot;
      }

      const stream = txn.runStream({
        ...sqlStatement,
        requestOptions,
        json: true,
        jsonOptions: { wrapNumbers: entityType != null },
      });

      for await (const row of stream) {
        // `undefined` may be sent by the `PartialResultStream` to test whether the consumer can accept more data.
        if (row === undefined) {
          continue;
        }

        yield entityType ? spannerObjectToInstance(row, entityType) : row;
      }
    } catch (error) {
      // If running in a provided transaction, the error will be caught by `snapshot()` or `transaction()`.
      // Otherwise, the error should be converted.
      throw transaction ? error : (convertSpannerToEntityError(error) ?? error);
    } finally {
      snapshot?.end();
    }
  }

  /**
   * Runs the given SQL statement in the database, returning an async iterable of batches of results.
   * By default, the statement is run in a {@link SpannerReadOnlyTransaction}. To perform a write operation, pass a
   * {@link SpannerReadWriteTransaction} in the options.
   *
   * @param options Options for the operation.
   * @param statement The SQL statement to run.
   * @returns An async iterable that yields batches of rows returned by the query.
   *   If {@link QueryOptions.entityType} is set, the rows are converted to instances of that class.
   */
  async *queryBatches<T>(
    options: QueryOptions<T> & {
      /**
       * The maximum number of items to include in each batch.
       */
      batchSize: number;
    },
    statement: SqlStatement,
  ): AsyncIterable<T[]> {
    const { batchSize } = options;
    let batch: T[] = [];

    for await (const item of this.queryStream(options, statement)) {
      batch.push(item);

      if (batch.length >= batchSize) {
        yield batch;
        batch = [];
      }
    }

    if (batch.length > 0) {
      yield batch;
    }
  }

  // Types that can be used as hints to disambiguate query parameter array types.
  static readonly ParamTypeFloat64Array: SqlParamType = {
    type: 'array',
    child: { type: 'float64' },
  };
  static readonly ParamTypeInt64Array: SqlParamType = {
    type: 'array',
    child: { type: 'int64' },
  };
  static readonly ParamTypeNumericArray: SqlParamType = {
    type: 'array',
    child: { type: 'numeric' },
  };
  static readonly ParamTypeBoolArray: SqlParamType = {
    type: 'array',
    child: { type: 'bool' },
  };
  static readonly ParamTypeStringArray: SqlParamType = {
    type: 'array',
    child: { type: 'string' },
  };
  static readonly ParamTypeBytesArray: SqlParamType = {
    type: 'array',
    child: { type: 'bytes' },
  };
  static readonly ParamTypeJsonArray: SqlParamType = {
    type: 'array',
    child: { type: 'json' },
  };
  static readonly ParamTypeTimestampArray: SqlParamType = {
    type: 'array',
    child: { type: 'timestamp' },
  };
  static readonly ParamTypeDateArray: SqlParamType = {
    type: 'array',
    child: { type: 'date' },
  };

  /**
   * Converts the given entity or array of entities to Spanner objects, grouping them by table name.
   *
   * @param entity The entity or array of entities to convert.
   * @returns A map where the keys are the table names and the values are the Spanner objects.
   */
  private entitiesToSpannerObjects(
    entity: object | object[],
  ): Record<string, object[]> {
    const entities = Array.isArray(entity) ? entity : [entity];

    return entities.reduce<Record<string, object[]>>((map, entity) => {
      const entityType = entity.constructor;
      const { tableName } = this.tableCache.getMetadata(entityType);
      const obj = instanceToSpannerObject(entity, entityType);
      map[tableName] = map[tableName] ?? [];
      map[tableName].push(obj);
      return map;
    }, {});
  }

  /**
   * Inserts the given entities into the database.
   * If the entities already exist (including if it is soft-deleted), an error will be thrown.
   *
   * Either a single entity or an array of entities can be provided.
   *
   * @param entity The entity or array of entities to insert.
   * @param options Options for the operation.
   */
  async insert(
    entity: object | object[],
    options: SpannerReadWriteTransactionOption = {},
  ): Promise<void> {
    const objs = this.entitiesToSpannerObjects(entity);

    await this.transaction(options, async (transaction) =>
      Object.entries(objs).forEach(([tableName, objs]) =>
        transaction.insert(tableName, objs),
      ),
    );
  }

  /**
   * Replaces the given entities in the database.
   * If the entity already exists, all columns are overwritten, even if they are not present in the entity (in the case
   * of nullable columns).
   *
   * Either a single entity or an array of entities can be provided. If an array is provided, all entities should
   * specify the same set of columns.
   *
   * @param entity The entity to write.
   * @param options Options for the operation.
   */
  async replace(
    entity: object | object[],
    options: SpannerReadWriteTransactionOption = {},
  ): Promise<void> {
    const objs = this.entitiesToSpannerObjects(entity);

    await this.transaction(options, async (transaction) =>
      Object.entries(objs).forEach(([tableName, objs]) =>
        transaction.replace(tableName, objs),
      ),
    );
  }

  /**
   * Updates the given entity in the database.
   *
   * The update should also contain the primary key of the entity.
   *
   * Unless `upsert` is set to `true`, an error will be thrown if the entity does not exist.
   * When `upsert` is `true`, all non-nullable columns must be present in the update.
   *
   * `includeSoftDeletes` can be set to `true` to update soft-deleted entities. If `includeSoftDeletes` is `false` (the
   * default), soft-deleted entities will not be updated, resulting in either an error or an insert, depending on the
   * value of `upsert`.
   *
   * @param entityType The type of entity to update.
   * @param update The columns to update, as well as the primary key.
   * @param options Options for the operation.
   * @returns The updated entity.
   */
  async update<T>(
    entityType: Type<T>,
    update: Partial<T>,
    options: SpannerReadWriteTransactionOption &
      Pick<FindOptions, 'includeSoftDeletes'> & {
        /**
         * A function that will be called with the entity before it is updated.
         * This function can throw an error to prevent the update.
         */
        validateFn?: (entity: T) => void;

        /**
         * If `true`, the entity will be inserted if it does not exist.
         */
        upsert?: boolean;
      } = {},
  ): Promise<T> {
    const primaryKey = this.getPrimaryKey(update, entityType);
    const { tableName } = this.tableCache.getMetadata(entityType);

    return await this.transaction(
      { transaction: options.transaction },
      async (transaction) => {
        const existingEntity = await this.findOneByKey(entityType, primaryKey, {
          transaction,
          includeSoftDeletes: options.includeSoftDeletes,
        });

        let updatedInstance: T;
        let operation: 'update' | 'insert';
        if (existingEntity) {
          if (options.validateFn) {
            options.validateFn(existingEntity);
          }

          updatedInstance = updateInstanceByColumn(existingEntity, update);
          operation = 'update';
        } else if (options.upsert) {
          updatedInstance = copyInstanceWithMissingColumnsToNull(
            update,
            entityType,
          );
          operation = 'insert';
        } else {
          throw new EntityNotFoundError(entityType, primaryKey);
        }

        const updateObj = instanceToSpannerObject(updatedInstance, entityType);
        transaction[operation](tableName, updateObj);
        return updatedInstance;
      },
    );
  }

  /**
   * Deletes the given entity from the database, or throws an error if it does not exist.
   *
   * To "hard-delete" an already soft-deleted entity, set `includeSoftDeletes` to `true`.
   *
   * @param entityType The type of entity to delete.
   * @param key The primary key of the entity to delete.
   * @param options Options for the operation.
   * @returns The deleted entity.
   */
  async delete<T>(
    entityType: Type<T>,
    key: SpannerKey | SpannerKey[number],
    options: SpannerReadWriteTransactionOption &
      Pick<FindOptions, 'includeSoftDeletes'> & {
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

    return await this.transaction(
      { transaction: options.transaction },
      async (transaction) => {
        const existingEntity = await this.findOneByKeyOrFail(entityType, key, {
          transaction,
          includeSoftDeletes: options.includeSoftDeletes,
        });

        if (options.validateFn) {
          options.validateFn(existingEntity);
        }

        transaction.deleteRows(tableName, [key as any]);

        return existingEntity;
      },
    );
  }
}

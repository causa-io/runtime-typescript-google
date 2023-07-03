import { Database, Snapshot, Transaction } from '@google-cloud/spanner';
import { Type } from '@google-cloud/spanner/build/src/codec.js';
import { TimestampBounds } from '@google-cloud/spanner/build/src/transaction.js';
import { spannerObjectToInstance } from './conversion.js';
import { convertSpannerToEntityError } from './error-converter.js';
import { TransactionFinishedError } from './errors.js';
import { SpannerTableCache } from './table-cache.js';

/**
 * Any Spanner transaction that can be used for reading.
 */
export type SpannerReadOnlyTransaction = Snapshot | Transaction;

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

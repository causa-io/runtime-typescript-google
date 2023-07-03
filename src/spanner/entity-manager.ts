import { Database, Snapshot, Transaction } from '@google-cloud/spanner';
import { TimestampBounds } from '@google-cloud/spanner/build/src/transaction.js';
import { convertSpannerToEntityError } from './error-converter.js';
import { TransactionFinishedError } from './errors.js';

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
 * A class that manages access to entities stored in a Cloud Spanner database.
 * Entities are defined by classes decorated with the `SpannerTable` and `SpannerColumn` decorators.
 */
export class SpannerEntityManager {
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
  async snapshot<T>(runFn: SnapshotFunction<T>): Promise<T>;
  /**
   * Runs the provided function in a read-only transaction ({@link Snapshot}).
   * The snapshot will be automatically released when the function returns.
   *
   * @param options The options to use when creating the snapshot.
   * @param runFn The function to run in the transaction.
   * @returns The return value of the function.
   */
  async snapshot<T>(
    options: SnapshotOptions,
    runFn: SnapshotFunction<T>,
  ): Promise<T>;
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
}

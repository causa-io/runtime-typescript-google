import { Database, Snapshot, Transaction } from '@google-cloud/spanner';
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
}

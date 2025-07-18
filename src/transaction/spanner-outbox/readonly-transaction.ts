import type {
  ReadOnlyStateTransaction,
  ReadOnlyTransactionOption,
} from '@causa/runtime';
import type { Type } from '@nestjs/common';
import {
  SpannerEntityManager,
  type SpannerReadOnlyTransaction,
  type SpannerReadWriteTransaction,
} from '../../spanner/index.js';

/**
 * Option for a function that accepts a {@link SpannerReadOnlyStateTransaction}.
 */
export type SpannerReadOnlyStateTransactionOption =
  ReadOnlyTransactionOption<SpannerReadOnlyStateTransaction>;

/**
 * A {@link ReadOnlyStateTransaction} that uses Spanner for state storage.
 */
export class SpannerReadOnlyStateTransaction
  implements ReadOnlyStateTransaction
{
  /**
   * Creates a new {@link SpannerReadOnlyStateTransaction}.
   *
   * @param entityManager The {@link SpannerEntityManager} to use to access entities in the state.
   * @param spannerTransaction The {@link SpannerReadWriteTransaction} or {@link SpannerReadOnlyTransaction} to use.
   */
  constructor(
    readonly entityManager: SpannerEntityManager,
    readonly spannerTransaction:
      | SpannerReadWriteTransaction
      | SpannerReadOnlyTransaction,
  ) {}

  async get<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | null> {
    const primaryKey = this.entityManager.getPrimaryKey(entity, type);
    const row = await this.entityManager.findOneByKey(type, primaryKey, {
      transaction: this.spannerTransaction,
      includeSoftDeletes: true,
    });
    return row ?? null;
  }
}

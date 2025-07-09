import type { StateTransaction } from '@causa/runtime';
import type { Type } from '@nestjs/common';
import {
  SpannerEntityManager,
  type SpannerReadWriteTransaction,
} from '../spanner/index.js';
import { SpannerReadOnlyStateTransaction } from './spanner-readonly-transaction.js';

/**
 * A {@link StateTransaction} that uses Spanner for state storage.
 */
export class SpannerStateTransaction
  extends SpannerReadOnlyStateTransaction
  implements StateTransaction
{
  /**
   * Creates a new {@link SpannerStateTransaction}.
   *
   * @param entityManager The {@link SpannerEntityManager} to use to access entities in the state.
   * @param spannerTransaction The {@link SpannerReadWriteTransaction} to use for the transaction.
   */
  constructor(
    readonly entityManager: SpannerEntityManager,
    readonly spannerTransaction: SpannerReadWriteTransaction,
  ) {
    super(entityManager, spannerTransaction);
  }

  async set<T extends object>(entity: T): Promise<void> {
    await this.entityManager.replace(entity, {
      transaction: this.spannerTransaction,
    });
  }

  async delete<T extends object>(
    typeOrEntity: Type<T> | T,
    key?: Partial<T>,
  ): Promise<void> {
    const type = (
      key === undefined ? typeOrEntity.constructor : typeOrEntity
    ) as Type<T>;
    key ??= typeOrEntity as Partial<T>;
    const primaryKey = this.entityManager.getPrimaryKey(key, type);

    await this.entityManager.delete(type, primaryKey, {
      transaction: this.spannerTransaction,
      includeSoftDeletes: true,
    });
  }
}

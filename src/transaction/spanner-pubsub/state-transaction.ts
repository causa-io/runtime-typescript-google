import { FindReplaceStateTransaction } from '@causa/runtime';
import { Transaction as SpannerTransaction } from '@google-cloud/spanner';
import { Type } from '@nestjs/common';
import { SpannerEntityManager } from '../../spanner/index.js';

/**
 * A {@link FindReplaceStateTransaction} that uses Spanner for state storage.
 */
export class SpannerStateTransaction implements FindReplaceStateTransaction {
  /**
   * Creates a new {@link SpannerStateTransaction}.
   *
   * @param entityManager The {@link SpannerEntityManager} to use to access entities in the state.
   * @param transaction The {@link SpannerTransaction} to use for the transaction.
   */
  constructor(
    readonly entityManager: SpannerEntityManager,
    readonly transaction: SpannerTransaction,
  ) {}

  async replace<T extends object>(entity: T): Promise<void> {
    await this.entityManager.replace(entity, { transaction: this.transaction });
  }

  async deleteWithSameKeyAs<T extends object>(
    type: Type<T>,
    key: Partial<T>,
  ): Promise<void> {
    const primaryKey = this.entityManager.getPrimaryKey(key, type);

    await this.entityManager.delete(type, primaryKey, {
      transaction: this.transaction,
      includeSoftDeletes: true,
    });
  }

  async findOneWithSameKeyAs<T extends object>(
    type: Type<T>,
    entity: Partial<T>,
  ): Promise<T | undefined> {
    const primaryKey = this.entityManager.getPrimaryKey(entity, type);

    return await this.entityManager.findOneByKey(type, primaryKey, {
      transaction: this.transaction,
      includeSoftDeletes: true,
    });
  }
}

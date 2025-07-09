import type { StateTransaction } from '@causa/runtime';
import type { Type } from '@nestjs/common';
import {
  SpannerEntityManager,
  type SpannerReadWriteTransaction,
} from '../spanner/index.js';

/**
 * A {@link StateTransaction} that uses Spanner for state storage.
 */
export class SpannerStateTransaction implements StateTransaction {
  /**
   * Creates a new {@link SpannerStateTransaction}.
   *
   * @param entityManager The {@link SpannerEntityManager} to use to access entities in the state.
   * @param transaction The {@link SpannerReadWriteTransaction} to use for the transaction.
   */
  constructor(
    readonly entityManager: SpannerEntityManager,
    readonly transaction: SpannerReadWriteTransaction,
  ) {}

  async set<T extends object>(entity: T): Promise<void> {
    await this.entityManager.replace(entity, { transaction: this.transaction });
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
      transaction: this.transaction,
      includeSoftDeletes: true,
    });
  }

  async get<T extends object>(
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

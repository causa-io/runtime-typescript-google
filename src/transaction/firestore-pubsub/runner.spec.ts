import {
  EntityNotFoundError,
  type Event,
  IsDateType,
  IsNullable,
  RetryableError,
  ValidateNestedType,
  type VersionedEntity,
  VersionedEntityManager,
} from '@causa/runtime';
import { AppFixture } from '@causa/runtime/nestjs/testing';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import { Injectable, Module } from '@nestjs/common';
import { IsString, IsUUID } from 'class-validator';
import {
  CollectionReference,
  Firestore,
  Transaction,
} from 'firebase-admin/firestore';
import 'jest-extended';
import { FirebaseModule } from '../../firebase/module.js';
import {
  FirestoreCollection,
  FirestoreCollectionsModule,
  getReferenceForFirestoreDocument,
} from '../../firestore/index.js';
import { FirestoreFixture } from '../../firestore/testing.js';
import { PubSubPublisherModule } from '../../pubsub/publisher.module.js';
import { FirebaseFixture, PubSubFixture } from '../../testing.js';
import { FirestorePubSubTransactionModule } from './module.js';
import { FirestoreReadOnlyStateTransaction } from './readonly-state-transaction.js';
import { FirestorePubSubTransactionRunner } from './runner.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';
import { FirestorePubSubTransaction } from './transaction.js';

@FirestoreCollection({ name: 'myDocuments', path: (doc) => doc.id })
@SoftDeletedFirestoreCollection()
class MyDocument implements VersionedEntity {
  constructor(data: Partial<MyDocument> = {}) {
    Object.assign(this, {
      id: '1234',
      createdAt: new Date(),
      updatedAt: new Date(),
      deletedAt: null,
      value: '🎉',
      ...data,
    });
  }

  @IsString()
  readonly id!: string;

  @IsDateType()
  readonly createdAt!: Date;

  @IsDateType()
  readonly updatedAt!: Date;

  @IsDateType()
  @IsNullable()
  readonly deletedAt!: Date | null;

  @IsString()
  readonly value!: string;
}

class MyEvent implements Event {
  @IsUUID()
  readonly id!: string;

  @IsDateType()
  readonly producedAt!: Date;

  @IsString()
  readonly name!: string;

  @ValidateNestedType(() => MyDocument)
  readonly data!: MyDocument;
}

@Injectable()
class MyEntityManager extends VersionedEntityManager<
  FirestorePubSubTransaction,
  FirestoreReadOnlyStateTransaction,
  MyEvent
> {
  constructor(runner: FirestorePubSubTransactionRunner) {
    super('my.entity.v1', MyEvent, MyDocument, runner);
  }
}

@Module({
  imports: [
    FirebaseModule.forRoot(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
    PubSubPublisherModule.forRoot(),
    FirestorePubSubTransactionModule.forRoot(),
  ],
  providers: [MyEntityManager],
})
class MyModule {}

describe('FirestorePubSubTransactionRunner', () => {
  let appFixture: AppFixture;
  let pubSubFixture: PubSubFixture;
  let firestore: Firestore;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;
  let runner: FirestorePubSubTransactionRunner;
  let myEntityManager: VersionedEntityManager<
    FirestorePubSubTransaction,
    FirestoreReadOnlyStateTransaction,
    MyEvent
  >;

  beforeAll(async () => {
    pubSubFixture = new PubSubFixture({ 'my.entity.v1': MyEvent });
    appFixture = new AppFixture(MyModule, {
      fixtures: [
        new FirebaseFixture(),
        new FirestoreFixture([MyDocument]),
        pubSubFixture,
      ],
    });
    await appFixture.init();
    firestore = appFixture.get(Firestore);
    runner = appFixture.get(FirestorePubSubTransactionRunner);
    myEntityManager = appFixture.get(MyEntityManager);
    activeCollection = appFixture.get(FirestoreFixture).collection(MyDocument);
    deletedCollection =
      runner.collectionResolver.getCollectionsForType(MyDocument).softDelete!
        .collection;
  });

  afterEach(() => appFixture.clear());

  afterAll(() => appFixture.delete());

  it('should commit the transaction and publish the events', async () => {
    const document = new MyDocument();
    const activeDocRef = getReferenceForFirestoreDocument(
      activeCollection,
      document,
    );
    const deletedDocRef = getReferenceForFirestoreDocument(
      deletedCollection,
      document,
    );
    await activeDocRef.set(document);

    const actualEvent = await runner.run(async (transaction) => {
      expect(transaction.firestoreTransaction).toBe(
        transaction.stateTransaction.firestoreTransaction,
      );

      return await myEntityManager.delete(
        '🗑️',
        { id: document.id },
        { transaction },
      );
    });

    const actualDocument = await deletedDocRef.get();
    expect(actualDocument.data()).toEqual({
      ...document,
      updatedAt: actualEvent.producedAt,
      deletedAt: actualEvent.producedAt,
      _expirationDate: new Date(
        actualEvent.producedAt.getTime() + 24 * 3600 * 1000,
      ),
    });
    const actualActiveDocument = await activeDocRef.get();
    expect(actualActiveDocument.exists).toBeFalse();
    await pubSubFixture.expectMessage(
      'my.entity.v1',
      expect.objectContaining({ event: actualEvent }),
    );
  });

  it('should not publish the events when an error is thrown within the transaction', async () => {
    const actualPromise = runner.run(async (transaction) => {
      await myEntityManager.create(
        '️🎉',
        { id: 'id', value: '👶' },
        { transaction },
      );

      throw new Error('💥');
    });

    await expect(actualPromise).rejects.toThrow('💥');
    const actualDocument = await activeCollection.doc('id').get();
    expect(actualDocument.exists).toBeFalse();
    await pubSubFixture.expectNoMessage('my.entity.v1');
  });

  it('should rethrow transient Firestore errors as retryable errors', async () => {
    const deadlineExceeded = new Error('🕰️');
    (deadlineExceeded as any).code = status.CANCELLED;
    jest
      .spyOn(firestore, 'runTransaction')
      .mockRejectedValueOnce(deadlineExceeded);

    const actualPromise = runner.run(async () => {});

    await expect(actualPromise).rejects.toThrow(RetryableError);
  });

  it('should convert Firestore errors to entity errors', async () => {
    const actualPromise = runner.run(async (transaction) => {
      transaction.firestoreTransaction.delete(activeCollection.doc('id'), {
        exists: true,
      });
    });

    await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
  });

  it('should run a readonly transaction', async () => {
    jest.spyOn(firestore, 'runTransaction');

    const actualResult = await runner.run(
      { readOnly: true },
      async (transaction) => {
        expect(transaction).toBeInstanceOf(FirestoreReadOnlyStateTransaction);
        expect(transaction.firestoreTransaction).toBeInstanceOf(Transaction);
        return '🎉';
      },
    );

    expect(actualResult).toEqual('🎉');
    expect(firestore.runTransaction).toHaveBeenCalledExactlyOnceWith(
      expect.any(Function),
      { readOnly: true },
    );
  });
});

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
import { Logger } from '@causa/runtime/nestjs';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import { IsString, IsUUID } from 'class-validator';
import {
  CollectionReference,
  Firestore,
  getFirestore,
} from 'firebase-admin/firestore';
import 'jest-extended';
import { getDefaultFirebaseApp } from '../../firebase/index.js';
import {
  FirestoreCollection,
  getReferenceForFirestoreDocument,
} from '../../firestore/index.js';
import {
  clearFirestoreCollection,
  createFirestoreTemporaryCollection,
} from '../../firestore/testing.js';
import { PubSubPublisher } from '../../pubsub/index.js';
import { PubSubFixture } from '../../pubsub/testing/index.js';
import { FirestorePubSubTransactionRunner } from './runner.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';
import type {
  FirestoreCollectionResolver,
  FirestoreCollectionsForDocumentType,
} from './state-transaction.js';
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

describe('FirestorePubSubTransactionRunner', () => {
  let logger: Logger;
  let pubSubFixture: PubSubFixture;
  let firestore: Firestore;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;
  let publisher: PubSubPublisher;
  let resolver: FirestoreCollectionResolver;
  let runner: FirestorePubSubTransactionRunner;
  let myEntityManager: VersionedEntityManager<
    FirestorePubSubTransaction,
    MyEvent
  >;

  beforeAll(async () => {
    logger = new Logger({});
    firestore = getFirestore(getDefaultFirebaseApp());
    pubSubFixture = new PubSubFixture();
    const pubSubConf = await pubSubFixture.create('my.entity.v1', MyEvent);
    publisher = new PubSubPublisher({
      configurationGetter: (key) => pubSubConf[key],
    });
    activeCollection = createFirestoreTemporaryCollection(
      firestore,
      MyDocument,
    );
    deletedCollection = createFirestoreTemporaryCollection(
      firestore,
      MyDocument,
    );
    resolver = {
      getCollectionsForType<T>(documentType: {
        new (): T;
      }): FirestoreCollectionsForDocumentType<any> {
        if (documentType !== MyDocument) {
          throw new Error('Unexpected document type.');
        }

        return {
          activeCollection,
          softDelete: {
            collection: deletedCollection,
            expirationDelay: 24 * 3600 * 1000,
            expirationField: '_expirationDate',
          },
        };
      },
    };
  });

  beforeEach(() => {
    runner = new FirestorePubSubTransactionRunner(
      firestore,
      publisher,
      resolver,
      logger,
    );
    myEntityManager = new VersionedEntityManager(
      'my.entity.v1',
      MyEvent,
      MyDocument,
      runner,
    );
  });

  afterEach(async () => {
    pubSubFixture.clear();
    await clearFirestoreCollection(activeCollection);
    await clearFirestoreCollection(deletedCollection);
  });

  afterAll(async () => {
    await pubSubFixture.deleteAll();
  });

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

    const [actualEvent] = await runner.run(async (transaction) => {
      expect(transaction.firestoreTransaction).toBe(
        transaction.stateTransaction.transaction,
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
    await pubSubFixture.expectMessageInTopic(
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
    await pubSubFixture.expectNoMessageInTopic('my.entity.v1');
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
});

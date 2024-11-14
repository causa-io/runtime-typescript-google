import type { EventPublisher, VersionedEntity } from '@causa/runtime';
import { InjectEventPublisher, LoggerModule } from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import { Test, TestingModule } from '@nestjs/testing';
import { FirebaseModule } from '../../firebase/index.js';
import {
  FirestoreCollection,
  FirestoreCollectionsModule,
} from '../../firestore/index.js';
import {
  getFirestoreCollectionFromModule,
  overrideFirestoreCollections,
} from '../../firestore/testing.js';
import { PubSubPublisherModule } from '../../pubsub/index.js';
import { FirestorePubSubTransactionModule } from './module.js';
import { FirestorePubSubTransactionRunner } from './runner.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';

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

  readonly id!: string;
  readonly createdAt!: Date;
  readonly updatedAt!: Date;
  readonly deletedAt!: Date | null;
  readonly value!: string;
}

@Injectable()
class MyService {
  constructor(
    @InjectEventPublisher()
    readonly publisher: EventPublisher,
    readonly runner: FirestorePubSubTransactionRunner,
  ) {}
}

describe('FirestorePubSubTransactionModule', () => {
  let testModule: TestingModule;

  beforeEach(async () => {
    let builder = Test.createTestingModule({
      providers: [MyService],
      imports: [
        ConfigModule.forRoot({ isGlobal: true }),
        LoggerModule,
        PubSubPublisherModule.forRoot(),
        FirebaseModule.forTesting(),
        FirestoreCollectionsModule.forRoot([MyDocument]),
        FirestorePubSubTransactionModule.forRoot(),
      ],
    });
    builder = overrideFirestoreCollections(MyDocument)(builder);
    testModule = await builder.compile();
  });

  afterEach(async () => {
    await testModule.close();
  });

  it('should expose the runner', async () => {
    const { runner: actualRunner } = testModule.get(MyService);
    const actualCollections =
      actualRunner.collectionResolver.getCollectionsForType(MyDocument);

    expect(actualRunner).toBeInstanceOf(FirestorePubSubTransactionRunner);
    expect(actualCollections.activeCollection).toBe(
      getFirestoreCollectionFromModule(testModule, MyDocument),
    );
    expect(actualCollections.softDelete?.collection.path).toEqual(
      `${actualCollections.activeCollection.path}$deleted`,
    );
  });
});

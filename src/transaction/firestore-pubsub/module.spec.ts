import type { EventPublisher, VersionedEntity } from '@causa/runtime';
import { InjectEventPublisher, LoggerModule } from '@causa/runtime/nestjs';
import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Injectable, Module } from '@nestjs/common';
import { ConfigModule } from '@nestjs/config';
import type { CollectionReference } from 'firebase-admin/firestore';
import { FirebaseModule } from '../../firebase/index.js';
import {
  FirestoreCollection,
  FirestoreCollectionsModule,
} from '../../firestore/index.js';
import { FirestoreFixture } from '../../firestore/testing.js';
import { PubSubPublisherModule } from '../../pubsub/index.js';
import { FirebaseFixture } from '../../testing.js';
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

@Module({
  providers: [MyService],
  imports: [
    ConfigModule.forRoot({ isGlobal: true }),
    LoggerModule.forRoot(),
    PubSubPublisherModule.forRoot(),
    FirebaseModule.forTesting(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
    FirestorePubSubTransactionModule.forRoot(),
  ],
})
export class MyModule {}

describe('FirestorePubSubTransactionModule', () => {
  let appFixture: AppFixture;
  let collection: CollectionReference<MyDocument>;

  beforeEach(async () => {
    appFixture = new AppFixture(MyModule, {
      fixtures: [new FirebaseFixture(), new FirestoreFixture([MyDocument])],
    });
    await appFixture.init();
    collection = appFixture.get(FirestoreFixture).collection(MyDocument);
  });

  afterEach(() => appFixture.delete());

  it('should expose the runner', async () => {
    const { runner: actualRunner } = appFixture.get(MyService);
    const actualCollections =
      actualRunner.collectionResolver.getCollectionsForType(MyDocument);

    expect(actualRunner).toBeInstanceOf(FirestorePubSubTransactionRunner);
    expect(actualCollections.activeCollection).toBe(collection);
    expect(actualCollections.softDelete?.collection.path).toEqual(
      `${actualCollections.activeCollection.path}$deleted`,
    );
  });
});

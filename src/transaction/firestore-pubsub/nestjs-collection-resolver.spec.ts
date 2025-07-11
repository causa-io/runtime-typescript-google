import type { VersionedEntity } from '@causa/runtime';
import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Module } from '@nestjs/common';
import { Transform } from 'class-transformer';
import {
  CollectionReference,
  Firestore,
  Timestamp,
} from 'firebase-admin/firestore';
import 'jest-extended';
import { FirebaseModule } from '../../firebase/index.js';
import {
  FirestoreCollection,
  FirestoreCollectionsModule,
  getReferenceForFirestoreDocument,
  makeFirestoreDataConverter,
} from '../../firestore/index.js';
import {
  clearFirestoreCollection,
  FirebaseFixture,
  FirestoreFixture,
} from '../../testing.js';
import { NestJsFirestoreCollectionResolver } from './nestjs-collection-resolver.js';
import { SoftDeletedFirestoreCollection } from './soft-deleted-collection.decorator.js';

@FirestoreCollection({ name: 'myDocuments', path: (doc) => doc.id })
@SoftDeletedFirestoreCollection({ deletedDocumentsCollectionSuffix: '$🗑️' })
class MyDocument implements VersionedEntity {
  constructor(data: Partial<MyDocument> = {}) {
    Object.assign(this, data);
  }

  readonly id!: string;
  readonly createdAt!: Date;
  readonly updatedAt!: Date;
  readonly deletedAt!: Date | null;
  @Transform(({ value }) => value.toUpperCase(), { toPlainOnly: true })
  @Transform(({ value }) => value.toLowerCase(), { toClassOnly: true })
  readonly value!: string;
}

@Module({
  imports: [
    FirebaseModule.forTesting(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
  ],
})
class GlobalModule {}

@Module({
  providers: [NestJsFirestoreCollectionResolver],
})
class MyModule {}

@Module({ imports: [GlobalModule, MyModule] })
class TestModule {}

describe('NestJsFirestoreCollectionResolver', () => {
  let appFixture: AppFixture;
  let firestore: Firestore;
  let resolver: NestJsFirestoreCollectionResolver;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;

  beforeEach(async () => {
    appFixture = new AppFixture(TestModule, {
      fixtures: [new FirebaseFixture(), new FirestoreFixture([MyDocument])],
    });
    await appFixture.init();
    firestore = appFixture.get(Firestore);
    activeCollection = appFixture.get(FirestoreFixture).collection(MyDocument);
    deletedCollection = firestore
      .collection(`${activeCollection.path}$🗑️`)
      .withConverter(makeFirestoreDataConverter(MyDocument));
    resolver = appFixture.get(NestJsFirestoreCollectionResolver);
  });

  afterEach(async () => {
    await clearFirestoreCollection(deletedCollection);
    await appFixture.delete();
  });

  describe('getCollectionsForType', () => {
    it('should return the active and deleted collections for the given type', () => {
      const actualCollections = resolver.getCollectionsForType(MyDocument);

      expect(actualCollections.activeCollection).toBe(activeCollection);
      expect(actualCollections.softDelete?.collection.path).toEqual(
        deletedCollection.path,
      );
      // It should be a temporary collection with a prefix.
      expect(actualCollections.softDelete?.collection.path).not.toStartWith(
        'myDocuments',
      );
    });

    it('should return an active collection with a converter', async () => {
      const { activeCollection } = resolver.getCollectionsForType(MyDocument);
      const expectedDocument = new MyDocument({
        id: '🎁',
        createdAt: new Date('2021-01-01'),
        updatedAt: new Date('2021-01-02'),
        deletedAt: null,
        value: 'omg 🎉',
      });
      const docRef = getReferenceForFirestoreDocument(
        activeCollection,
        expectedDocument,
      );
      await docRef.set(expectedDocument);

      const actualStoredDocument = (
        await firestore
          .collection(activeCollection.path)
          .doc(expectedDocument.id)
          .get()
      ).data();
      const actualRetrievedDocument = (await docRef.get()).data();

      expect(actualStoredDocument).toEqual({
        id: '🎁',
        createdAt: Timestamp.fromDate(expectedDocument.createdAt),
        updatedAt: Timestamp.fromDate(expectedDocument.updatedAt),
        deletedAt: null,
        value: 'OMG 🎉',
      });
      expect(actualRetrievedDocument).toEqual(expectedDocument);
      expect(actualRetrievedDocument).toBeInstanceOf(MyDocument);
    });

    it('should return a deleted collection with a converter', async () => {
      const { softDelete } = resolver.getCollectionsForType(MyDocument);
      const deletedAt = new Date('2021-01-02');
      const expectedDocument = new MyDocument({
        id: '🎁',
        createdAt: new Date('2021-01-01'),
        updatedAt: new Date('2021-01-02'),
        deletedAt,
        value: 'omg 🎉',
      });
      const docRef = getReferenceForFirestoreDocument(
        softDelete?.collection as any,
        expectedDocument,
      );
      await docRef.set(expectedDocument);

      const actualStoredDocument = (
        await firestore
          .collection(softDelete?.collection.path ?? '')
          .doc(expectedDocument.id)
          .get()
      ).data();
      const actualRetrievedDocument = (await docRef.get()).data();

      expect(actualStoredDocument).toEqual({
        id: '🎁',
        createdAt: Timestamp.fromDate(expectedDocument.createdAt),
        updatedAt: Timestamp.fromDate(expectedDocument.updatedAt),
        deletedAt: Timestamp.fromDate(deletedAt),
        value: 'OMG 🎉',
      });
      expect(actualRetrievedDocument).toEqual(expectedDocument);
      expect(actualRetrievedDocument).toBeInstanceOf(MyDocument);
    });
  });
});

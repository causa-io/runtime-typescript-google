import { VersionedEntity } from '@causa/runtime';
import { Module } from '@nestjs/common';
import { Test, TestingModule } from '@nestjs/testing';
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
import { getFirestoreCollectionInjectionName } from '../../firestore/inject-collection.decorator.js';
import {
  clearFirestoreCollection,
  overrideFirestoreCollections,
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

describe('NestJsFirestoreCollectionResolver', () => {
  let testModule: TestingModule;
  let firestore: Firestore;
  let resolver: NestJsFirestoreCollectionResolver;
  let activeCollection: CollectionReference<MyDocument>;
  let deletedCollection: CollectionReference<MyDocument>;

  beforeEach(async () => {
    let builder = Test.createTestingModule({
      imports: [GlobalModule, MyModule],
    });
    builder = overrideFirestoreCollections(builder, MyDocument);
    testModule = await builder.compile();
    firestore = testModule.get(Firestore);
    activeCollection = testModule.get(
      getFirestoreCollectionInjectionName(MyDocument),
    );
    deletedCollection = firestore
      .collection(`${activeCollection.path}$🗑️`)
      .withConverter(makeFirestoreDataConverter(MyDocument));
    resolver = testModule.get(NestJsFirestoreCollectionResolver);
  });

  afterEach(async () => {
    await clearFirestoreCollection(activeCollection);
    await clearFirestoreCollection(deletedCollection);
    await testModule.close();
  });

  describe('getCollectionsForType', () => {
    it('should return the active and deleted collections for the given type', () => {
      const actualCollections = resolver.getCollectionsForType(MyDocument);

      expect(actualCollections.activeCollection).toBe(activeCollection);
      expect(actualCollections.deletedCollection.path).toEqual(
        deletedCollection.path,
      );
      // It should be a temporary collection with a prefix.
      expect(actualCollections.deletedCollection.path).not.toStartWith(
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
      const { deletedCollection } = resolver.getCollectionsForType(MyDocument);
      const deletedAt = new Date('2021-01-02');
      const expectedDocument = new MyDocument({
        id: '🎁',
        createdAt: new Date('2021-01-01'),
        updatedAt: new Date('2021-01-02'),
        deletedAt,
        value: 'omg 🎉',
      });
      const docRef = getReferenceForFirestoreDocument(
        deletedCollection,
        expectedDocument,
      );
      await docRef.set(expectedDocument);

      const actualStoredDocument = (
        await firestore
          .collection(deletedCollection.path)
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

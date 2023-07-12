import { createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { Injectable, Module } from '@nestjs/common';
import { CollectionReference } from 'firebase-admin/firestore';
import 'jest-extended';
import { FirebaseModule } from '../index.js';
import { FirestoreCollection } from './collection.decorator.js';
import { FirestoreCollectionsModule } from './collections.module.js';
import { InjectFirestoreCollection } from './inject-collection.decorator.js';
import {
  getFirestoreCollectionFromModule,
  overrideFirestoreCollections,
} from './testing.js';

@FirestoreCollection({ name: 'myCol', path: (doc) => doc.id })
class MyDocument {
  constructor(readonly id: string = '1234') {}
}

@Injectable()
class TestService {
  constructor(
    @InjectFirestoreCollection(MyDocument)
    readonly myCol: CollectionReference<MyDocument>,
  ) {}
}

@Module({
  imports: [
    FirebaseModule.forTesting(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
  ],
  providers: [TestService],
})
class MyModule {}

describe('testing', () => {
  describe('overrideFirestoreCollections', () => {
    it('should override the collection name with a prefix during tests', async () => {
      const testApp = await createApp(MyModule, {
        appFactory: makeTestAppFactory({
          overrides: overrideFirestoreCollections(MyDocument),
        }),
      });
      const testService = testApp.get(TestService);
      const document = new MyDocument('❄️');

      const actualCollection = testService.myCol;
      await actualCollection.doc('someDoc').set(document);
      const actualDocument = (
        await actualCollection.doc('someDoc').get()
      ).data();

      expect(actualDocument).toBeInstanceOf(MyDocument);
      expect(actualDocument).toEqual({ id: '❄️' });
      expect(actualCollection).toBeInstanceOf(CollectionReference);
      expect(actualCollection.path).toEndWith('-myCol');
      expect(actualCollection.path).not.toStartWith('-myCol');
    });
  });

  describe('getFirestoreCollectionFromModule', () => {
    it('should retrieve the collection from the test module', async () => {
      const testApp = await createApp(MyModule, {
        appFactory: makeTestAppFactory({
          overrides: overrideFirestoreCollections(MyDocument),
        }),
      });
      const testService = testApp.get(TestService);
      const expectedCollection = testService.myCol;

      const actualCollection = getFirestoreCollectionFromModule(
        testApp,
        MyDocument,
      );

      expect(actualCollection).toBe(expectedCollection);
    });
  });
});

import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Injectable, Module } from '@nestjs/common';
import { CollectionReference } from 'firebase-admin/firestore';
import 'jest-extended';
import { FirebaseFixture } from '../firebase/testing.js';
import { FirebaseModule } from '../index.js';
import { FirestoreCollection } from './collection.decorator.js';
import { FirestoreCollectionsModule } from './collections.module.js';
import { InjectFirestoreCollection } from './inject-collection.decorator.js';
import { FirestoreFixture } from './testing.js';

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

describe('FirestoreFixture', () => {
  let appFixture: AppFixture;
  let fixture: FirestoreFixture;
  let service: TestService;

  beforeAll(async () => {
    fixture = new FirestoreFixture([MyDocument]);
    appFixture = new AppFixture(MyModule, {
      fixtures: [new FirebaseFixture(), fixture],
    });
    await appFixture.init();
    service = appFixture.get(TestService);
  });

  describe('init', () => {
    it('should override the collection name with a prefix during tests', async () => {
      const document = new MyDocument('❄️');

      const actualCollection = service.myCol;
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

  describe('clear', () => {
    it('should clear the collection', async () => {
      const actualCollection = fixture.collection(MyDocument);
      await actualCollection.doc('test').set(new MyDocument('test'));

      await fixture.clear();

      const actualDocument = await actualCollection.doc('test').get();
      expect(actualDocument.exists).toBeFalse();
    });
  });

  describe('collection', () => {
    it('should retrieve the collection from the test module', async () => {
      const actualCollection = fixture.collection(MyDocument);

      expect(actualCollection).toBe(service.myCol);
    });
  });
});

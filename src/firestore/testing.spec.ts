import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Injectable, Module } from '@nestjs/common';
import { CollectionReference, Firestore } from 'firebase-admin/firestore';
import 'jest-extended';
import { FirebaseFixture } from '../firebase/testing.js';
import { FirebaseModule } from '../index.js';
import { FirestoreCollection } from './collection.decorator.js';
import { FirestoreFixture } from './testing.js';

@FirestoreCollection({ path: (doc) => ['myCol', doc.id] })
class MyDocument {
  constructor(readonly id: string = '1234') {}
}

@Injectable()
class TestService {
  constructor(readonly firestore: Firestore) {}
}

@Module({
  imports: [FirebaseModule.forTesting()],
  providers: [TestService],
})
class MyModule {}

describe('FirestoreFixture', () => {
  let appFixture: AppFixture;
  let fixture: FirestoreFixture;
  let service: TestService;

  beforeAll(async () => {
    fixture = new FirestoreFixture();
    appFixture = new AppFixture(MyModule, {
      fixtures: [new FirebaseFixture(), fixture],
    });
    await appFixture.init();
    service = appFixture.get(TestService);
  });

  afterAll(() => appFixture.delete());

  describe('init', () => {
    it('should override the Firestore instance with one using a random database', async () => {
      expect(fixture.databaseId).toStartWith('test-');
      expect(service.firestore.databaseId).toEqual(fixture.databaseId);
      expect(fixture.firestore).toBe(service.firestore);
    });

    it('should use a database that can be read and written', async () => {
      const document = new MyDocument('❄️');

      const actualCollection = fixture.collection(MyDocument);
      await actualCollection.doc(document.id).set(document);
      const actualDocument = (
        await actualCollection.doc(document.id).get()
      ).data();

      expect(actualDocument).toBeInstanceOf(MyDocument);
      expect(actualDocument).toEqual({ id: '❄️' });
    });
  });

  describe('clear', () => {
    it('should clear all the documents in the database', async () => {
      const actualCollection = fixture.collection(MyDocument);
      const docRef1 = actualCollection.doc('test');
      const docRef2 = actualCollection
        .doc('test')
        .collection('subCollection')
        .doc('nested');
      await docRef1.set(new MyDocument('test'));
      await docRef2.set({ value: '🐑' });

      await fixture.clear();

      const actualDocument = await docRef1.get();
      expect(actualDocument.exists).toBeFalse();
      const actualNestedDocument = await docRef2.get();
      expect(actualNestedDocument.exists).toBeFalse();
    });
  });

  describe('collection', () => {
    it('should return the collection in the test database', async () => {
      const actualCollection = fixture.collection(MyDocument);

      expect(actualCollection).toBeInstanceOf(CollectionReference);
      expect(actualCollection.path).toEqual('myCol');
      expect(actualCollection.firestore).toBe(fixture.firestore);
    });
  });
});

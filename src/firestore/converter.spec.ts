import {
  CollectionReference,
  Timestamp,
  getFirestore,
} from 'firebase-admin/firestore';
import { getDefaultFirebaseApp } from '../firebase/index.js';
import { FirestoreCollection } from './collection.decorator.js';
import {
  clearFirestoreCollection,
  createFirestoreTemporaryCollection,
} from './testing.js';

@FirestoreCollection({ name: 'someCollection', path: (doc) => doc.field1 })
class SomeDocument {
  constructor(data: Partial<SomeDocument> = {}) {
    Object.assign(this, {
      field1: '🎁',
      field2: new Date('2021-01-01'),
      ...data,
    });
  }

  readonly field1!: string;

  readonly field2!: Date;
}

describe('converter', () => {
  describe('makeFirestoreDataConverter', () => {
    const app = getDefaultFirebaseApp();
    const firestore = getFirestore(app);
    let collection: CollectionReference<SomeDocument>;

    beforeAll(async () => {
      collection = createFirestoreTemporaryCollection(firestore, SomeDocument);
    });

    afterEach(async () => {
      await clearFirestoreCollection(firestore, collection);
    });

    it('should transform the class to a plain object and store it', async () => {
      const document = new SomeDocument();

      await collection.doc('test').set(document);

      const actualDocument = await firestore
        .collection(collection.path)
        .doc('test')
        .get();
      expect(actualDocument.data()).toEqual({
        field1: '🎁',
        field2: new Timestamp(document.field2.getTime() / 1000, 0),
      });
    });

    it('should accept an already plain object and store it', async () => {
      const document: SomeDocument = {
        field1: '🪄',
        field2: new Date('2022-01-01'),
      };

      await collection.doc('test').set(document);

      const actualDocument = await firestore
        .collection(collection.path)
        .doc('test')
        .get();
      expect(actualDocument.data()).toEqual({
        field1: '🪄',
        field2: new Timestamp(document.field2.getTime() / 1000, 0),
      });
    });

    it('should return the class when reading a document', async () => {
      const document = new SomeDocument();

      await collection.doc('test').set(document);

      const actualDocument = await collection.doc('test').get();
      const actualData = actualDocument.data();
      expect(actualData).toBeInstanceOf(SomeDocument);
      expect(actualData).toEqual(document);
      expect(actualData?.field2).toBeInstanceOf(Date);
    });
  });
});

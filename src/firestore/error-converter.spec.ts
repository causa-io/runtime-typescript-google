import {
  EntityAlreadyExistsError,
  EntityNotFoundError,
  IncorrectEntityVersionError,
} from '@causa/runtime';
import { status } from '@grpc/grpc-js';
import {
  CollectionReference,
  Firestore,
  Timestamp,
  getFirestore,
} from 'firebase-admin/firestore';
import { getDefaultFirebaseApp } from '../firebase/index.js';
import { FirestoreCollection } from './collection.decorator.js';
import { wrapFirestoreOperation } from './error-converter.js';
import { TemporaryFirestoreError } from './errors.js';
import {
  clearFirestoreCollection,
  createFirestoreTemporaryCollection,
} from './testing.js';

@FirestoreCollection({ name: 'tmpCol', path: (doc) => doc.id })
class MyDocument {
  constructor(readonly id: string) {}
}

describe('error converter', () => {
  let firestore: Firestore;
  let collection: CollectionReference<MyDocument>;

  beforeAll(() => {
    firestore = getFirestore(getDefaultFirebaseApp());
    collection = createFirestoreTemporaryCollection(firestore, MyDocument);
  });

  afterEach(async () => {
    await clearFirestoreCollection(collection);
  });

  describe('wrapFirestoreOperation', () => {
    it('should run the operation and return the result', async () => {
      const actualResult = await wrapFirestoreOperation(async () => {
        await collection.doc('test').set(new MyDocument('test'));
        return '🐑';
      });

      expect(actualResult).toEqual('🐑');
      const actualDocument = await collection.doc('test').get();
      expect(actualDocument.data()).toEqual({ id: 'test' });
    });

    it('should rethrow an unknown error', async () => {
      const actualPromise = wrapFirestoreOperation(async () => {
        throw new Error('🐑');
      });

      expect(actualPromise).rejects.toThrow('🐑');
    });

    it('should throw an EntityNotFoundError', async () => {
      const actualPromise = wrapFirestoreOperation(async () => {
        await collection.doc('test').update({ id: 'test' });
      });

      await expect(actualPromise).rejects.toThrow(EntityNotFoundError);
    });

    it('should throw an EntityAlreadyExistsError', async () => {
      await collection.doc('test').set(new MyDocument('test'));

      const actualPromise = wrapFirestoreOperation(async () => {
        await collection.doc('test').create(new MyDocument('test'));
      });

      await expect(actualPromise).rejects.toThrow(EntityAlreadyExistsError);
    });

    it('should throw an IncorrectEntityVersionError', async () => {
      await collection.doc('test').set(new MyDocument('test'));

      const actualPromise = wrapFirestoreOperation(async () => {
        await collection.doc('test').delete({
          lastUpdateTime: new Timestamp(0, 0),
        });
      });

      await expect(actualPromise).rejects.toThrow(IncorrectEntityVersionError);
    });

    it('should throw a TemporaryFirestoreError', async () => {
      const errorCodes = [
        status.CANCELLED,
        status.DEADLINE_EXCEEDED,
        status.INTERNAL,
        status.UNAVAILABLE,
      ];

      for (const code of errorCodes) {
        const actualPromise = wrapFirestoreOperation(async () => {
          const error = new Error('🐑');
          (error as any).code = code;
          throw error;
        });

        await expect(actualPromise).rejects.toThrow(TemporaryFirestoreError);
      }
    });
  });
});

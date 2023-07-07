import { Firestore, getFirestore } from 'firebase-admin/firestore';
import 'reflect-metadata';
import { getDefaultFirebaseApp } from '../firebase/index.js';
import {
  FirestoreCollection,
  getFirestoreCollectionMetadataForType,
  getReferenceForFirestoreDocument,
} from './collection.decorator.js';

@FirestoreCollection({
  name: '🔖',
  path: (doc) => doc.id,
})
class MyDocument {
  constructor(readonly id: string = '🐑') {}
}

describe('FirestoreCollection', () => {
  let firestore: Firestore;

  beforeAll(() => {
    firestore = getFirestore(getDefaultFirebaseApp());
  });

  describe('getFirestoreCollectionMetadataForType', () => {
    it('should return the name of the Firestore collection corresponding to the given class', () => {
      const actualMetadata = getFirestoreCollectionMetadataForType(MyDocument);

      expect(actualMetadata.name).toEqual('🔖');
      expect(actualMetadata.path({ id: '🎁' })).toEqual('🎁');
    });

    it('should throw if the class is not decorated with FirestoreCollection', () => {
      class MyDocument {}

      expect(() => getFirestoreCollectionMetadataForType(MyDocument)).toThrow(
        `Class 'MyDocument' is not declared as a Firestore collection.`,
      );
    });
  });

  describe('getReferenceForFirestoreDocument', () => {
    it('should return the reference for the Firestore document corresponding to the given document', () => {
      const document = new MyDocument();

      const actualReference = getReferenceForFirestoreDocument(
        firestore.collection('🗃️'),
        document,
      );

      expect(actualReference.path).toEqual('🗃️/🐑');
    });

    it('should return the reference for the Firestore document corresponding to the given partial document', () => {
      const document = { id: '🎁/🗃️/🪆' };

      const actualReference = getReferenceForFirestoreDocument(
        firestore.collection('🗃️'),
        document,
        MyDocument,
      );

      expect(actualReference.path).toEqual('🗃️/🎁/🗃️/🪆');
    });

    it('should throw if the document is not decorated with FirestoreCollection', () => {
      class MyDocument {}

      expect(() =>
        getReferenceForFirestoreDocument(
          firestore.collection('🗃️'),
          new MyDocument(),
        ),
      ).toThrow(
        `Class 'MyDocument' is not declared as a Firestore collection.`,
      );
    });

    it('should throw if the returned path is undefined', () => {
      expect(() =>
        getReferenceForFirestoreDocument(
          firestore.collection('🗃️'),
          {},
          MyDocument,
        ),
      ).toThrow(
        `The path of the 'MyDocument' document cannot be obtained from the given object.`,
      );
    });
  });
});

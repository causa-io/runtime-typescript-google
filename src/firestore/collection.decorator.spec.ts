import { initializeApp } from 'firebase-admin/app';
import { Firestore, getFirestore } from 'firebase-admin/firestore';
import 'reflect-metadata';
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

@FirestoreCollection({
  name: '🔖',
  path: (doc) => [doc.grandParentId, doc.parentId, doc.id],
})
class MyDocumentWithArrayPath {
  constructor(
    readonly grandParentId: string | undefined = '📁',
    readonly parentId: string | null = '🗃️',
    readonly id: string = '🐑',
    readonly unused?: string,
  ) {}
}

describe('FirestoreCollection', () => {
  let firestore: Firestore;

  beforeAll(() => {
    firestore = getFirestore(initializeApp());
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

    it('should return the reference when the path function returns an array', () => {
      const document = new MyDocumentWithArrayPath();

      const actualReference = getReferenceForFirestoreDocument(
        firestore.collection('🗃️'),
        document,
      );

      expect(actualReference.path).toEqual('🗃️/📁/🗃️/🐑');
    });

    it('should return the reference for a partial document with array path', () => {
      const document = { grandParentId: '🎁', parentId: '🗃️', id: '🪆' };

      const actualReference = getReferenceForFirestoreDocument(
        firestore.collection('🗃️'),
        document,
        MyDocumentWithArrayPath,
      );

      expect(actualReference.path).toEqual('🗃️/🎁/🗃️/🪆');
    });

    it('should throw if the returned path array contains undefined', () => {
      expect(() =>
        getReferenceForFirestoreDocument(
          firestore.collection('🗃️'),
          { parentId: '🗃️', id: '🐑' },
          MyDocumentWithArrayPath,
        ),
      ).toThrow(
        `The path of the 'MyDocumentWithArrayPath' document cannot be obtained from the given object.`,
      );
    });

    it('should throw if the returned path array contains null', () => {
      expect(() =>
        getReferenceForFirestoreDocument(
          firestore.collection('🗃️'),
          { grandParentId: '🎁', parentId: null, id: '🐑' },
          MyDocumentWithArrayPath,
        ),
      ).toThrow(
        `The path of the 'MyDocumentWithArrayPath' document cannot be obtained from the given object.`,
      );
    });
  });
});

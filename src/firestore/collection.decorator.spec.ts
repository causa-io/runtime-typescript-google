import 'reflect-metadata';
import {
  FirestoreCollection,
  getFirestoreCollectionMetadataForType,
} from './collection.decorator.js';

describe('FirestoreCollection', () => {
  it('should return the name of the Firestore collection corresponding to the given class', () => {
    @FirestoreCollection({
      name: '🔖',
      path: (doc) => doc.id,
    })
    class MyDocument {
      id!: string;
    }

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

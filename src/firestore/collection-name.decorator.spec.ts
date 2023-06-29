import 'reflect-metadata';
import {
  FirestoreCollectionName,
  getFirestoreCollectionNameForType,
} from './collection-name.decorator.js';

describe('FirestoreCollectionName', () => {
  it('should return the name of the Firestore collection corresponding to the given class', () => {
    @FirestoreCollectionName('🔖')
    class MyDocument {}

    const actualName = getFirestoreCollectionNameForType(MyDocument);

    expect(actualName).toEqual('🔖');
  });

  it('should throw if the class is not decorated with FirestoreCollectionName', () => {
    class MyDocument {}

    expect(() => getFirestoreCollectionNameForType(MyDocument)).toThrow(
      `Class 'MyDocument' is not declared as a Firestore collection.`,
    );
  });
});

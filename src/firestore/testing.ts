import { NestJsModuleOverrider } from '@causa/runtime/nestjs/testing';
import { INestApplicationContext, Type } from '@nestjs/common';
import { CollectionReference, Firestore } from 'firebase-admin/firestore';
import * as uuid from 'uuid';
import { getFirestoreCollectionMetadataForType } from './collection.decorator.js';
import { makeFirestoreDataConverter } from './converter.js';
import { getFirestoreCollectionInjectionName } from './inject-collection.decorator.js';

/**
 * Creates a new collection prefixed with a random ID.
 *
 * @param firestore The {@link Firestore} instance to use.
 * @param documentType The type of the document stored in the collection.
 *   It should be decorated with `FirestoreCollection`.
 * @returns The {@link CollectionReference} of the created collection.
 */
export function createFirestoreTemporaryCollection<T>(
  firestore: Firestore,
  documentType: Type<T>,
): CollectionReference<T> {
  const prefix = `${uuid.v4().slice(-10)}-`;
  const { name } = getFirestoreCollectionMetadataForType(documentType);
  return firestore
    .collection(`${prefix}${name}`)
    .withConverter(makeFirestoreDataConverter(documentType));
}

/**
 * Clears a Firestore collection of all its documents.
 *
 * @param collectionRef The reference to the collection that should be cleared.
 */
export async function clearFirestoreCollection(
  collectionRef: CollectionReference,
): Promise<void> {
  const batch = collectionRef.firestore.batch();

  const documents = await collectionRef.listDocuments();

  documents.forEach((document) => batch.delete(document));

  await batch.commit();
}

/**
 * Overrides the providers for Firestore collections with temporary collections.
 *
 * @param documentTypes The types of documents corresponding to Firestore collections, for which collections should be
 *   overridden.
 * @returns The {@link NestJsModuleOverrider} that can be used to override the Firestore collections.
 */
export function overrideFirestoreCollections(
  ...documentTypes: Type[]
): NestJsModuleOverrider {
  return (builder) => {
    documentTypes.forEach((documentType) => {
      builder = builder
        .overrideProvider(getFirestoreCollectionInjectionName(documentType))
        .useFactory({
          factory: (firestore: Firestore) =>
            createFirestoreTemporaryCollection(firestore, documentType),
          inject: [Firestore],
        });
    });

    return builder;
  };
}

/**
 * Returns the {@link CollectionReference} of the Firestore collection corresponding to the given class.
 * Retrieving it from the module or application ensures the "mocked" collection is used.
 *
 * @param context The NestJS module or application from which the Firestore collection should be retrieved.
 * @param documentType The type of document stored in the collection.
 * @returns The {@link CollectionReference} of the Firestore collection.
 */
export function getFirestoreCollectionFromModule<T>(
  context: INestApplicationContext,
  documentType: Type<T>,
): CollectionReference<T> {
  return context.get(getFirestoreCollectionInjectionName(documentType));
}

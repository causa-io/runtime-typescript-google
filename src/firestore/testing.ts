import type {
  AppFixture,
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import type { Type } from '@nestjs/common';
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
  documents.forEach((d) => batch.delete(d));
  await batch.commit();
}

/**
 * A {@link Fixture} that replaces Firestore collections with temporary collections, and clears them when requested.
 */
export class FirestoreFixture implements Fixture {
  /**
   * The parent {@link AppFixture}.
   */
  private appFixture!: AppFixture;

  constructor(
    /**
     * The types of documents that should be stored in temporary collections and cleared.
     */
    readonly types: Type[],
  ) {}

  async init(appFixture: AppFixture): Promise<NestJsModuleOverrider> {
    this.appFixture = appFixture;

    return (builder) =>
      this.types.reduce(
        (builder, t) =>
          builder
            .overrideProvider(getFirestoreCollectionInjectionName(t))
            .useFactory({
              factory: (f: Firestore) =>
                createFirestoreTemporaryCollection(f, t),
              inject: [Firestore],
            }),
        builder,
      );
  }

  async clear(): Promise<void> {
    await Promise.all(
      this.types.map((t) => clearFirestoreCollection(this.collection(t))),
    );
  }

  async delete(): Promise<void> {
    this.appFixture = undefined as any;
  }

  /**
   * The underlying {@link Firestore} instance used by this fixture.
   */
  get firestore(): Firestore {
    return this.appFixture.get(Firestore);
  }

  /**
   * Returns the (temporary) collection for the given document type.
   *
   * @param documentType The type of the document.
   * @returns The {@link CollectionReference} for the given document type.
   */
  collection<T>(documentType: Type<T>): CollectionReference<T> {
    return this.appFixture.get(
      getFirestoreCollectionInjectionName(documentType),
    );
  }
}

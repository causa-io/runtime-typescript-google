import { Injectable, type Type } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { CollectionReference } from 'firebase-admin/firestore';
import { makeFirestoreDataConverter } from '../../firestore/index.js';
import { getFirestoreCollectionInjectionName } from '../../firestore/inject-collection.decorator.js';
import { getSoftDeletedFirestoreCollectionMetadataForType } from './soft-deleted-collection.decorator.js';
import type {
  FirestoreCollectionResolver,
  FirestoreCollectionsForDocumentType,
} from './types.js';

/**
 * A {@link FirestoreCollectionResolver} that uses NestJS dependency injection to resolve Firestore collections.
 */
@Injectable()
export class NestJsFirestoreCollectionResolver
  implements FirestoreCollectionResolver
{
  /**
   * A cache of Firestore collections for document types.
   */
  private readonly collectionsByType: Map<
    Type,
    FirestoreCollectionsForDocumentType<any>
  > = new Map();

  /**
   * Creates a resolver that returns the Firestore collections for a given document type using NestJS dependency
   * injection.
   *
   * @param moduleRef The {@link ModuleRef} instance, used to dynamically resolve Firestore collections.
   */
  constructor(readonly moduleRef: ModuleRef) {}

  getCollectionsForType<T>(
    documentType: Type<T>,
  ): FirestoreCollectionsForDocumentType<T> {
    const existingDocumentCollections =
      this.collectionsByType.get(documentType);
    if (existingDocumentCollections) {
      return existingDocumentCollections;
    }

    const activeCollection: CollectionReference<T> = this.moduleRef.get(
      getFirestoreCollectionInjectionName(documentType),
      { strict: false },
    );

    const softDeleteMetadata =
      getSoftDeletedFirestoreCollectionMetadataForType(documentType);

    const documentCollections: FirestoreCollectionsForDocumentType<T> = {
      activeCollection,
      softDelete: softDeleteMetadata
        ? {
            collection: activeCollection.firestore
              .collection(
                `${activeCollection.path}${softDeleteMetadata.deletedDocumentsCollectionSuffix}`,
              )
              .withConverter(makeFirestoreDataConverter(documentType)),
            expirationDelay: softDeleteMetadata.expirationDelay,
            expirationField: softDeleteMetadata.expirationField,
          }
        : null,
    };

    this.collectionsByType.set(documentType, documentCollections);
    return documentCollections;
  }
}

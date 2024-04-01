import { VersionedEntity } from '@causa/runtime';
import {
  SoftDeletedFirestoreCollection,
  getSoftDeletedFirestoreCollectionMetadataForType,
} from './soft-deleted-collection.decorator.js';

describe('SoftDeletedFirestoreCollection', () => {
  describe('getSoftDeletedCollectionMetadataForType', () => {
    it('should return the default metadata values', () => {
      @SoftDeletedFirestoreCollection()
      class MyDocument implements VersionedEntity {
        createdAt!: Date;
        updatedAt!: Date;
        deletedAt!: Date | null;
      }

      const actualMetadata =
        getSoftDeletedFirestoreCollectionMetadataForType(MyDocument);

      expect(actualMetadata).toEqual({
        expirationDelay: 24 * 3600 * 1000,
        expirationField: '_expirationDate',
        deletedDocumentsCollectionSuffix: '$deleted',
      });
    });

    it('should return the metadata values from the decorator', () => {
      @SoftDeletedFirestoreCollection({
        expirationDelay: 42,
        expirationField: '⏰',
        deletedDocumentsCollectionSuffix: '🗑️',
      })
      class MyDocument implements VersionedEntity {
        createdAt!: Date;
        updatedAt!: Date;
        deletedAt!: Date | null;
      }

      const actualMetadata =
        getSoftDeletedFirestoreCollectionMetadataForType(MyDocument);

      expect(actualMetadata).toEqual({
        expirationDelay: 42,
        expirationField: '⏰',
        deletedDocumentsCollectionSuffix: '🗑️',
      });
    });

    it('should return null if the class is not decorated with SoftDeletedFirestoreCollection', () => {
      class MyDocument {}

      const actualMetadata =
        getSoftDeletedFirestoreCollectionMetadataForType(MyDocument);

      expect(actualMetadata).toBeNull();
    });
  });
});

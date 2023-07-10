import { DynamicModule, FactoryProvider } from '@nestjs/common';
import { Firestore } from 'firebase-admin/firestore';
import { getFirestoreCollectionMetadataForType } from './collection.decorator.js';
import { makeFirestoreDataConverter } from './converter.js';
import { getFirestoreCollectionInjectionName } from './inject-collection.decorator.js';

/**
 * A module that provides Firestore collections that can be injected in services using `InjectFirestoreCollection`.
 * The `FirebaseModule` must be imported and accessible in the current context.
 */
export class FirestoreCollectionsModule {
  /**
   * Creates a module that provides Firestore collections that can be injected in services using
   * `InjectFirestoreCollection`.
   * The `FirebaseModule` must be imported and accessible in the current context.
   *
   * @param documentTypes The types of document corresponding to Firestore collections.
   *   They should be decorated with `FirestoreCollection`.
   * @returns The module.
   */
  static forRoot(documentTypes: { new (): any }[]): DynamicModule {
    const providers: FactoryProvider[] = documentTypes.map((documentType) => ({
      provide: getFirestoreCollectionInjectionName(documentType),
      useFactory: (firestore: Firestore) =>
        firestore
          .collection(getFirestoreCollectionMetadataForType(documentType).name)
          .withConverter(makeFirestoreDataConverter(documentType)),
      inject: [Firestore],
    }));

    return {
      module: FirestoreCollectionsModule,
      providers,
      exports: providers.map((p) => p.provide),
      global: true,
    };
  }
}

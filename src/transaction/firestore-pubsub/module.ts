import { Logger } from '@causa/runtime/nestjs';
import { DynamicModule } from '@nestjs/common';
import { ModuleRef } from '@nestjs/core';
import { Firestore } from 'firebase-admin/firestore';
import { PubSubPublisher } from '../../index.js';
import { NestJsFirestoreCollectionResolver } from './nestjs-collection-resolver.js';
import { FirestorePubSubTransactionRunner } from './runner.js';

/**
 * The module exposing the {@link FirestorePubSubTransactionRunner}.
 * This modules assumes that the `FirebaseModule`, `FirestoreCollectionsModule` (for the relevant collections), and
 * `PubSubPublisherModule` are available.
 */
export class FirestorePubSubTransactionModule {
  /**
   * Creates a global module that provides the {@link FirestorePubSubTransactionRunner}.
   * This modules assumes that the `FirebaseModule`, `FirestoreCollectionsModule` (for the relevant collections), and
   * `PubSubPublisherModule` are available.
   *
   * @returns The module.
   */
  static forRoot(): DynamicModule {
    return {
      module: FirestorePubSubTransactionModule,
      global: true,
      providers: [
        {
          provide: FirestorePubSubTransactionRunner,
          useFactory: (
            firestore: Firestore,
            publisher: PubSubPublisher,
            moduleRef: ModuleRef,
            logger: Logger,
          ) => {
            const resolver = new NestJsFirestoreCollectionResolver(moduleRef);
            return new FirestorePubSubTransactionRunner(
              firestore,
              publisher,
              resolver,
              logger,
            );
          },
          inject: [Firestore, PubSubPublisher, ModuleRef, Logger],
        },
      ],
      exports: [FirestorePubSubTransactionRunner],
    };
  }
}

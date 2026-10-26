import type { DynamicModule } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import type { FirestoreStateTransactionOptions } from './readonly-state-transaction.js';
import {
  FIRESTORE_STATE_TRANSACTION_OPTIONS_INJECTION_NAME,
  FirestorePubSubTransactionRunner,
} from './runner.js';

/**
 * Options for the {@link FirestorePubSubTransactionModule}.
 * Options that are not set are read from the environment, e.g. `FIRESTORE_SEQUENTIAL_SOFT_DELETE_LOOKUPS=true`.
 */
export type FirestorePubSubTransactionModuleOptions =
  FirestoreStateTransactionOptions;

/**
 * The module exposing the {@link FirestorePubSubTransactionRunner}.
 * This modules assumes that the `FirebaseModule`, `PubSubPublisherModule`, and {@link ConfigService} are available.
 */
export class FirestorePubSubTransactionModule {
  /**
   * Creates a global module that provides the {@link FirestorePubSubTransactionRunner}.
   * This modules assumes that the `FirebaseModule`, `PubSubPublisherModule`, and {@link ConfigService} are available.
   *
   * @param options Options for the state transactions created by the runner.
   * @returns The module.
   */
  static forRoot(
    options: FirestorePubSubTransactionModuleOptions = {},
  ): DynamicModule {
    return {
      module: FirestorePubSubTransactionModule,
      global: true,
      providers: [
        {
          provide: FIRESTORE_STATE_TRANSACTION_OPTIONS_INJECTION_NAME,
          useFactory: (
            config: ConfigService,
          ): FirestoreStateTransactionOptions => ({
            sequentialSoftDeleteLookups:
              options.sequentialSoftDeleteLookups ??
              config.get<string>('FIRESTORE_SEQUENTIAL_SOFT_DELETE_LOOKUPS') ===
                'true',
          }),
          inject: [ConfigService],
        },
        FirestorePubSubTransactionRunner,
      ],
      exports: [FirestorePubSubTransactionRunner],
    };
  }
}

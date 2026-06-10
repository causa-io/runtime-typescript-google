import type { DynamicModule } from '@nestjs/common';
import { FirestorePubSubTransactionRunner } from './runner.js';

/**
 * The module exposing the {@link FirestorePubSubTransactionRunner}.
 * This modules assumes that the `FirebaseModule` and `PubSubPublisherModule` are available.
 */
export class FirestorePubSubTransactionModule {
  /**
   * Creates a global module that provides the {@link FirestorePubSubTransactionRunner}.
   * This modules assumes that the `FirebaseModule` and `PubSubPublisherModule` are available.
   *
   * @returns The module.
   */
  static forRoot(): DynamicModule {
    return {
      module: FirestorePubSubTransactionModule,
      global: true,
      providers: [FirestorePubSubTransactionRunner],
      exports: [FirestorePubSubTransactionRunner],
    };
  }
}

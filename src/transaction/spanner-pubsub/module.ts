import type { DynamicModule } from '@nestjs/common';
import { SpannerPubSubTransactionRunner } from './runner.js';

/**
 * The module exposing the {@link SpannerPubSubTransactionRunner}.
 * This modules assumes that the `SpannerModule` and `PubSubPublisherModule` are available.
 */
export class SpannerPubSubTransactionModule {
  /**
   * Create a global module that provides the {@link SpannerPubSubTransactionRunner}.
   * This modules assumes that the `SpannerModule` and `PubSubPublisherModule` are available.
   *
   * @returns The module.
   */
  static forRoot(): DynamicModule {
    return {
      module: SpannerPubSubTransactionModule,
      global: true,
      providers: [SpannerPubSubTransactionRunner],
      exports: [SpannerPubSubTransactionRunner],
    };
  }
}

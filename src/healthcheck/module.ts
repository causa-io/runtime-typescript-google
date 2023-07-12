import { terminusModuleWithLogger } from '@causa/runtime/nestjs';
import { Module } from '@nestjs/common';
import { PubSubHealthIndicator } from '../pubsub/index.js';
import { SpannerHealthIndicator } from '../spanner/index.js';
import { GoogleHealthcheckController } from './controller.js';

/**
 * A module implementing the healthcheck endpoint, checking the health of Google Cloud services, namely:
 *
 * - Pub/Sub
 * - Spanner
 *
 * This module expects the `@causa/runtime` `LoggerModule` to be imported, as well as the `SpannerModule` and
 * `PubSubPublisherModule` from this package.
 *
 * This module can be used if the only relevant checks are for Google Cloud services. Otherwise, implement a custom
 * healthcheck controller using {@link PubSubHealthIndicator} and {@link SpannerHealthIndicator}.
 */
@Module({
  controllers: [GoogleHealthcheckController],
  providers: [PubSubHealthIndicator, SpannerHealthIndicator],
  imports: [terminusModuleWithLogger],
})
export class GoogleHealthcheckModule {}

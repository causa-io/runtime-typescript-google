import { HEALTHCHECK_ENDPOINT, Public } from '@causa/runtime/nestjs';
import { Controller, Get } from '@nestjs/common';
import { HealthCheckResult, HealthCheckService } from '@nestjs/terminus';
import { PubSubHealthIndicator } from '../pubsub/index.js';
import { SpannerHealthIndicator } from '../spanner/index.js';

/**
 * A controller implementing the healthcheck endpoint, checking the health of Google Cloud services, namely:
 *
 * - Pub/Sub
 * - Spanner
 */
@Controller(HEALTHCHECK_ENDPOINT)
export class GoogleHealthcheckController {
  constructor(
    private health: HealthCheckService,
    private pubSubHealthIndicator: PubSubHealthIndicator,
    private spannerHealthIndicator: SpannerHealthIndicator,
  ) {}

  /**
   * Checks the health of the Google Cloud services.
   *
   * @returns The health of the Google Cloud services.
   */
  @Get()
  @Public()
  async healthCheck(): Promise<HealthCheckResult> {
    return await this.health.check([
      () => this.pubSubHealthIndicator.isHealthy(),
      () => this.spannerHealthIndicator.isHealthy(),
    ]);
  }
}

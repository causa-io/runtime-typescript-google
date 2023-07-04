import { Database } from '@google-cloud/spanner';
import { Injectable } from '@nestjs/common';
import {
  HealthCheckError,
  HealthIndicator,
  HealthIndicatorResult,
} from '@nestjs/terminus';

/**
 * The key used to identify the Spanner health indicator.
 */
const SPANNER_HEALTH_KEY = 'spanner';

/**
 * A service testing the availability of the Spanner service.
 */
@Injectable()
export class SpannerHealthIndicator extends HealthIndicator {
  constructor(private readonly database: Database) {
    super();
  }

  /**
   * Checks the health of the Spanner service by running a dummy query (`SELECT 1`).
   *
   * @returns The health of the Spanner service.
   */
  async isHealthy(): Promise<HealthIndicatorResult> {
    try {
      await this.database.run('SELECT 1');

      return this.getStatus(SPANNER_HEALTH_KEY, true);
    } catch (error: any) {
      throw new HealthCheckError(
        'Failed to run healthcheck Spanner query.',
        this.getStatus(SPANNER_HEALTH_KEY, false, { error: error.message }),
      );
    }
  }
}

import { BaseHealthIndicatorService } from '@causa/runtime/nestjs';
import { Database } from '@google-cloud/spanner';
import { Injectable } from '@nestjs/common';
import { HealthCheckError, type HealthIndicatorResult } from '@nestjs/terminus';

/**
 * The key used to identify the Spanner health indicator.
 */
const SPANNER_HEALTH_KEY = 'google.spanner';

/**
 * A service testing the availability of the Spanner service.
 */
@Injectable()
export class SpannerHealthIndicator extends BaseHealthIndicatorService {
  constructor(private readonly database: Database) {
    super();
  }

  async check(): Promise<HealthIndicatorResult> {
    try {
      await this.database.run('SELECT 1');

      return this.getStatus(SPANNER_HEALTH_KEY, true);
    } catch (error: any) {
      throw new HealthCheckError(
        'Failed to check health by running Spanner query.',
        this.getStatus(SPANNER_HEALTH_KEY, false, { error: error.message }),
      );
    }
  }
}

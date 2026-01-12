import { orFallbackFn, tryMap } from '@causa/runtime';
import {
  BaseHealthIndicatorService,
  type HealthChecker,
} from '@causa/runtime/nestjs';
import { Database } from '@google-cloud/spanner';
import { Injectable } from '@nestjs/common';
import {
  HealthIndicatorService,
  type HealthIndicatorResult,
} from '@nestjs/terminus';

/**
 * The key used to identify the Spanner health indicator.
 */
const SPANNER_HEALTH_KEY = 'google.spanner';

/**
 * A service testing the availability of the Spanner service.
 */
@Injectable()
export class SpannerHealthIndicator
  extends BaseHealthIndicatorService
  implements HealthChecker
{
  constructor(
    private readonly database: Database,
    private readonly healthIndicatorService: HealthIndicatorService,
  ) {
    super();
  }

  async check(): Promise<HealthIndicatorResult> {
    const check = this.healthIndicatorService.check(SPANNER_HEALTH_KEY);
    return await tryMap<HealthIndicatorResult>(
      async () => {
        await this.database.run('SELECT 1');
        return check.up();
      },
      orFallbackFn((error: any) => check.down({ error: error.message })),
    );
  }
}

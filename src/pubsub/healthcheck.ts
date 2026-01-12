import { orFallbackFn, tryMap } from '@causa/runtime';
import {
  BaseHealthIndicatorService,
  type HealthChecker,
} from '@causa/runtime/nestjs';
import { PubSub } from '@google-cloud/pubsub';
import { status } from '@grpc/grpc-js';
import { Injectable } from '@nestjs/common';
import {
  HealthIndicatorService,
  type HealthIndicatorResult,
} from '@nestjs/terminus';

/**
 * The key used to identify the Pub/Sub health indicator.
 */
const PUBSUB_HEALTH_KEY = 'google.pubSub';

/**
 * A service testing the availability of the Pub/Sub service.
 */
@Injectable()
export class PubSubHealthIndicator
  extends BaseHealthIndicatorService
  implements HealthChecker
{
  constructor(
    private readonly pubSub: PubSub,
    private readonly healthIndicatorService: HealthIndicatorService,
  ) {
    super();
  }

  async check(): Promise<HealthIndicatorResult> {
    const check = this.healthIndicatorService.check(PUBSUB_HEALTH_KEY);
    return await tryMap<HealthIndicatorResult>(
      async () => {
        await this.pubSub.getTopics({ autoPaginate: false, pageSize: 1 });
        return check.up();
      },
      orFallbackFn((error: any) => {
        // Permission denied errors are treated as healthy, as they indicate that the service could successfully connect
        // to the Pub/Sub service (which is probably healthy), but was not allowed to list topics (which is usually not
        // the case for publishers).
        return error.code === status.PERMISSION_DENIED
          ? check.up()
          : check.down({ error: error.message });
      }),
    );
  }
}

import { PubSub } from '@google-cloud/pubsub';
import { status } from '@grpc/grpc-js';
import { Injectable } from '@nestjs/common';
import {
  HealthCheckError,
  HealthIndicator,
  HealthIndicatorResult,
} from '@nestjs/terminus';

/**
 * The key used to identify the Pub/Sub health indicator.
 */
const PUBSUB_HEALTH_KEY = 'pubSub';

/**
 * A service testing the availability of the Pub/Sub service.
 */
@Injectable()
export class PubSubHealthIndicator extends HealthIndicator {
  constructor(private readonly pubSub: PubSub) {
    super();
  }

  /**
   * Checks the health of the Pub/Sub service by listing (at most one) topic.
   *
   * @returns The health of the Pub/Sub service.
   */
  async isHealthy(): Promise<HealthIndicatorResult> {
    try {
      await this.pubSub.getTopics({
        autoPaginate: false,
        pageSize: 1,
      });
    } catch (error: any) {
      // Permission denied errors are treated as healthy, as they indicate that the service could successfully connect
      // to the Pub/Sub service (which is probably healthy), but was not allowed to list topics (which is usually not
      // the case for publishers).
      if (error.code !== status.PERMISSION_DENIED) {
        throw new HealthCheckError(
          'Failed to check health by retrieving Pub/Sub topics.',
          this.getStatus(PUBSUB_HEALTH_KEY, false, { error: error.message }),
        );
      }
    }

    return this.getStatus(PUBSUB_HEALTH_KEY, true);
  }
}

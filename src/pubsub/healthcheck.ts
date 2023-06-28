import { PubSub } from '@google-cloud/pubsub';
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

      return this.getStatus(PUBSUB_HEALTH_KEY, true);
    } catch (error: any) {
      throw new HealthCheckError(
        'Failed to retrieve Pub/Sub topics.',
        this.getStatus(PUBSUB_HEALTH_KEY, false, { error: error.message }),
      );
    }
  }
}

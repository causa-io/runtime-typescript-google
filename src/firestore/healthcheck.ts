import { orFallbackFn, tryMap } from '@causa/runtime';
import {
  BaseHealthIndicatorService,
  type HealthChecker,
} from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import {
  HealthIndicatorService,
  type HealthIndicatorResult,
} from '@nestjs/terminus';
import { Firestore } from 'firebase-admin/firestore';

/**
 * The key used to identify the Firestore health indicator.
 */
const FIRESTORE_HEALTH_KEY = 'google.firestore';

/**
 * A service testing the availability of the Firestore service.
 */
@Injectable()
export class FirestoreHealthIndicator
  extends BaseHealthIndicatorService
  implements HealthChecker
{
  constructor(
    private readonly firestore: Firestore,
    private readonly healthIndicatorService: HealthIndicatorService,
  ) {
    super();
  }

  async check(): Promise<HealthIndicatorResult> {
    const check = this.healthIndicatorService.check(FIRESTORE_HEALTH_KEY);
    return await tryMap<HealthIndicatorResult>(
      async () => {
        await this.firestore.listCollections();
        return check.up();
      },
      orFallbackFn((error: any) => check.down({ error: error.message })),
    );
  }
}

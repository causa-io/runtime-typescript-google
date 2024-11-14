import { BaseHealthIndicatorService } from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import { HealthCheckError, type HealthIndicatorResult } from '@nestjs/terminus';
import { Firestore } from 'firebase-admin/firestore';

/**
 * The key used to identify the Firestore health indicator.
 */
const FIRESTORE_HEALTH_KEY = 'google.firestore';

/**
 * A service testing the availability of the Firestore service.
 */
@Injectable()
export class FirestoreHealthIndicator extends BaseHealthIndicatorService {
  constructor(private readonly firestore: Firestore) {
    super();
  }

  async check(): Promise<HealthIndicatorResult> {
    try {
      await this.firestore.listCollections();
      return this.getStatus(FIRESTORE_HEALTH_KEY, true);
    } catch (error: any) {
      throw new HealthCheckError(
        'Failed to check health by listing Firestore collections.',
        this.getStatus(FIRESTORE_HEALTH_KEY, false, {
          error: error.message,
        }),
      );
    }
  }
}

import { BaseHealthIndicatorService } from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import { HealthCheckError, HealthIndicatorResult } from '@nestjs/terminus';
import { Firestore } from 'firebase-admin/firestore';
import { FirestoreAdminClient } from '../firebase/index.js';

/**
 * The key used to identify the Firestore health indicator.
 */
const FIRESTORE_HEALTH_KEY = 'google.firestore';

/**
 * A service testing the availability of the Firestore service.
 */
@Injectable()
export class FirestoreHealthIndicator extends BaseHealthIndicatorService {
  /**
   * The ID of the database used by Firestore.
   */
  private readonly databaseId: string;

  constructor(
    private readonly admin: FirestoreAdminClient,
    firestore: Firestore,
  ) {
    super();

    this.databaseId = firestore.databaseId;
  }

  async check(): Promise<HealthIndicatorResult> {
    try {
      const projectId = await this.admin.getProjectId();
      const name = this.admin.databasePath(projectId, this.databaseId);
      await this.admin.getDatabase({ name });

      return this.getStatus(FIRESTORE_HEALTH_KEY, true);
    } catch (error: any) {
      throw new HealthCheckError(
        'Failed to check health by getting Firestore database.',
        this.getStatus(FIRESTORE_HEALTH_KEY, false, {
          error: error.message,
        }),
      );
    }
  }
}

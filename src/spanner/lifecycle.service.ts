import { Database, Spanner } from '@google-cloud/spanner';
import { Injectable, OnApplicationShutdown } from '@nestjs/common';

/**
 * A private service that handles the graceful shutdown of the Spanner Database.
 * Should be imported in the `SpannerModule`.
 */
@Injectable()
export class SpannerLifecycleService implements OnApplicationShutdown {
  constructor(
    private readonly spanner: Spanner,
    private readonly database: Database,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    await this.database.close();
    this.spanner.close();
  }
}

import { Logger } from '@causa/runtime/nestjs';
import { Database, Spanner } from '@google-cloud/spanner';
import { SessionLeakError } from '@google-cloud/spanner/build/src/session-pool.js';
import { Injectable, type OnApplicationShutdown } from '@nestjs/common';

/**
 * A private service that handles the graceful shutdown of the Spanner Database.
 * Should be imported in the `SpannerModule`.
 */
@Injectable()
export class SpannerLifecycleService implements OnApplicationShutdown {
  constructor(
    private readonly spanner: Spanner,
    private readonly database: Database,
    private readonly logger: Logger,
  ) {}

  async onApplicationShutdown(): Promise<void> {
    try {
      await this.database.close();
      this.spanner.close();
    } catch (error: any) {
      this.logger.error(
        {
          error: error.stack,
          spannerLeaks:
            error instanceof SessionLeakError ? error.messages : undefined,
        },
        'Failed to close Spanner client.',
      );
    }
  }
}

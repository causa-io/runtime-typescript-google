import { createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { Controller, Get, INestApplication, Module } from '@nestjs/common';
import { HealthCheckService, TerminusModule } from '@nestjs/terminus';
import 'jest-extended';
import { Logger } from 'nestjs-pino';
import supertest from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
import { SpannerHealthIndicator } from './healthcheck.js';
import { SpannerModule } from './module.js';
import { createDatabase } from './testing.js';

@Controller()
export class HealthController {
  constructor(
    private health: HealthCheckService,
    private spannerHealthIndicator: SpannerHealthIndicator,
  ) {}

  @Get()
  async healthCheck() {
    return await this.health.check([
      () => this.spannerHealthIndicator.isHealthy(),
    ]);
  }
}

@Module({
  controllers: [HealthController],
  imports: [
    TerminusModule.forRoot({ logger: Logger }),
    SpannerModule.forRoot(),
  ],
  providers: [SpannerHealthIndicator],
})
export class HealthModule {}

describe('SpannerHealthIndicator', () => {
  let database: Database;
  let app: INestApplication;
  let request: TestAgent<supertest.Test>;

  beforeAll(async () => {
    database = await createDatabase();
  });

  beforeEach(async () => {
    app = await createApp(HealthModule, {
      appFactory: makeTestAppFactory({
        overrides: (builder) =>
          builder.overrideProvider(Database).useValue(database),
      }),
    });
    request = supertest(app.getHttpServer());
  });

  afterAll(async () => {
    await database.delete();
  });

  it('should return 200 if the Spanner client is healthy', async () => {
    await request.get('/').expect(200, {
      status: 'ok',
      info: { spanner: { status: 'up' } },
      error: {},
      details: { spanner: { status: 'up' } },
    });
  });

  it('should return 503 if the Spanner client is unhealthy', async () => {
    jest.spyOn(database as any, 'run').mockRejectedValue(new Error('💥'));

    await request.get('/').expect(503, {
      status: 'error',
      info: {},
      error: { spanner: { status: 'down', error: '💥' } },
      details: { spanner: { status: 'down', error: '💥' } },
    });
    expect(database.run).toHaveBeenCalledExactlyOnceWith('SELECT 1');
  });
});

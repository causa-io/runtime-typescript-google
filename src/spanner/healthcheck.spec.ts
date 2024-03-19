import { HealthCheckModule, createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { INestApplication, Module } from '@nestjs/common';
import 'jest-extended';
import supertest from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
import { SpannerHealthIndicator } from './healthcheck.js';
import { SpannerModule } from './module.js';
import { createDatabase } from './testing.js';

@Module({
  imports: [
    SpannerModule.forRoot(),
    HealthCheckModule.forIndicators([SpannerHealthIndicator]),
  ],
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

  afterEach(async () => {
    app.close();
  });

  afterAll(async () => {
    await database.delete();
  });

  it('should return 200 if the Spanner client is healthy', async () => {
    await request.get('/health').expect(200, {
      status: 'ok',
      info: { 'google.spanner': { status: 'up' } },
      error: {},
      details: { 'google.spanner': { status: 'up' } },
    });
  });

  it('should return 503 if the Spanner client is unhealthy', async () => {
    jest.spyOn(database as any, 'run').mockRejectedValue(new Error('💥'));

    await request.get('/health').expect(503, {
      status: 'error',
      info: {},
      error: { 'google.spanner': { status: 'down', error: '💥' } },
      details: { 'google.spanner': { status: 'down', error: '💥' } },
    });
    expect(database.run).toHaveBeenCalledExactlyOnceWith('SELECT 1');
  });
});

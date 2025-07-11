import { HealthCheckModule } from '@causa/runtime/nestjs';
import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { Module } from '@nestjs/common';
import 'jest-extended';
import { SpannerHealthIndicator } from './healthcheck.js';
import { SpannerModule } from './module.js';
import { SpannerFixture } from './testing.js';

@Module({
  imports: [
    SpannerModule.forRoot(),
    HealthCheckModule.forIndicators([SpannerHealthIndicator]),
  ],
})
export class HealthModule {}

describe('SpannerHealthIndicator', () => {
  let appFixture: AppFixture;
  let database: Database;

  beforeEach(async () => {
    appFixture = new AppFixture(HealthModule, {
      fixtures: [new SpannerFixture()],
    });
    await appFixture.init();
    database = appFixture.get(Database);
  });

  afterEach(() => appFixture.delete());

  it('should return 200 if the Spanner client is healthy', async () => {
    await appFixture.request.get('/health').expect(200, {
      status: 'ok',
      info: { 'google.spanner': { status: 'up' } },
      error: {},
      details: { 'google.spanner': { status: 'up' } },
    });
  });

  it('should return 503 if the Spanner client is unhealthy', async () => {
    jest.spyOn(database as any, 'run').mockRejectedValue(new Error('💥'));

    await appFixture.request.get('/health').expect(503, {
      status: 'error',
      info: {},
      error: { 'google.spanner': { status: 'down', error: '💥' } },
      details: { 'google.spanner': { status: 'down', error: '💥' } },
    });
    expect(database.run).toHaveBeenCalledExactlyOnceWith('SELECT 1');
  });
});

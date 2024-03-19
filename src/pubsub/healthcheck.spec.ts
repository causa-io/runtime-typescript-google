import { HealthCheckModule, createApp } from '@causa/runtime/nestjs';
import { PubSub } from '@google-cloud/pubsub';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import { INestApplication, Module } from '@nestjs/common';
import supertest from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
import { PubSubHealthIndicator } from './healthcheck.js';
import { PubSubPublisherModule } from './publisher.module.js';

@Module({
  imports: [
    HealthCheckModule.forIndicators([PubSubHealthIndicator]),
    PubSubPublisherModule.forRoot(),
  ],
})
export class HealthModule {}

describe('PubSubHealthIndicator', () => {
  let app: INestApplication;
  let request: TestAgent<supertest.Test>;

  beforeEach(async () => {
    app = await createApp(HealthModule);
    request = supertest(app.getHttpServer());
  });

  afterEach(async () => {
    await app.close();
  });

  it('should return 200 if the Pub/Sub client is healthy', async () => {
    await request.get('/health').expect(200, {
      status: 'ok',
      info: { 'google.pubSub': { status: 'up' } },
      error: {},
      details: { 'google.pubSub': { status: 'up' } },
    });
  });

  it('should treat permission errors as healthy', async () => {
    const pubSub = app.get(PubSub);
    jest.spyOn(pubSub as any, 'getTopics').mockRejectedValue({
      code: status.PERMISSION_DENIED,
      message: 'Permission denied',
    });

    await request.get('/health').expect(200, {
      status: 'ok',
      info: { 'google.pubSub': { status: 'up' } },
      error: {},
      details: { 'google.pubSub': { status: 'up' } },
    });
  });

  it('should return 503 if the Pub/Sub client is unhealthy', async () => {
    const pubSub = app.get(PubSub);
    jest.spyOn(pubSub as any, 'getTopics').mockRejectedValue(new Error('💥'));

    await request.get('/health').expect(503, {
      status: 'error',
      info: {},
      error: { 'google.pubSub': { status: 'down', error: '💥' } },
      details: { 'google.pubSub': { status: 'down', error: '💥' } },
    });
  });
});

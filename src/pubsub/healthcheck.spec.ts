import { createApp } from '@causa/runtime/nestjs';
import { PubSub } from '@google-cloud/pubsub';
import { status } from '@grpc/grpc-js';
import { jest } from '@jest/globals';
import { Controller, Get, INestApplication, Module } from '@nestjs/common';
import { HealthCheckService, TerminusModule } from '@nestjs/terminus';
import { Logger } from 'nestjs-pino';
import supertest from 'supertest';
import { PubSubHealthIndicator } from './healthcheck.js';
import { PubSubPublisherModule } from './publisher.module.js';

@Controller()
export class HealthController {
  constructor(
    private health: HealthCheckService,
    private pubSubHealthIndicator: PubSubHealthIndicator,
  ) {}

  @Get()
  async healthCheck() {
    return await this.health.check([
      () => this.pubSubHealthIndicator.isHealthy(),
    ]);
  }
}

@Module({
  controllers: [HealthController],
  imports: [
    TerminusModule.forRoot({ logger: Logger }),
    PubSubPublisherModule.forRoot(),
  ],
  providers: [PubSubHealthIndicator],
})
export class HealthModule {}

describe('PubSubHealthIndicator', () => {
  let app: INestApplication;
  let request: supertest.SuperTest<supertest.Test>;

  beforeEach(async () => {
    app = await createApp(HealthModule);
    request = supertest(app.getHttpServer());
  });

  it('should return 200 if the Pub/Sub client is healthy', async () => {
    await request.get('/').expect(200, {
      status: 'ok',
      info: { pubSub: { status: 'up' } },
      error: {},
      details: { pubSub: { status: 'up' } },
    });
  });

  it('should treat permission errors as healthy', async () => {
    const pubSub = app.get(PubSub);
    jest.spyOn(pubSub as any, 'getTopics').mockRejectedValue({
      code: status.PERMISSION_DENIED,
      message: 'Permission denied',
    });

    await request.get('/').expect(200, {
      status: 'ok',
      info: { pubSub: { status: 'up' } },
      error: {},
      details: { pubSub: { status: 'up' } },
    });
  });

  it('should return 503 if the Pub/Sub client is unhealthy', async () => {
    const pubSub = app.get(PubSub);
    jest.spyOn(pubSub as any, 'getTopics').mockRejectedValue(new Error('💥'));

    await request.get('/').expect(503, {
      status: 'error',
      info: {},
      error: { pubSub: { status: 'down', error: '💥' } },
      details: { pubSub: { status: 'down', error: '💥' } },
    });
  });
});

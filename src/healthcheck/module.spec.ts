import { AuthModule, LoggerModule, createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { PubSub } from '@google-cloud/pubsub';
import { Database } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { INestApplication, Module } from '@nestjs/common';
import supertest from 'supertest';
import { PubSubPublisherModule } from '../pubsub/index.js';
import { SpannerModule } from '../spanner/index.js';
import { GoogleHealthcheckModule } from './module.js';

@Module({
  imports: [
    AuthModule,
    LoggerModule,
    SpannerModule.forRoot(),
    PubSubPublisherModule.forRoot(),
    GoogleHealthcheckModule,
  ],
})
class MyModule {}

describe('GoogleHealthcheckModule', () => {
  let app: INestApplication;
  let request: supertest.SuperTest<supertest.Test>;

  const runMock = jest.fn(() => Promise.resolve());
  const getTopicsMock = jest.fn(() => Promise.resolve([]));

  beforeAll(async () => {
    app = await createApp(MyModule, {
      appFactory: makeTestAppFactory({
        overrides: (builder) =>
          builder
            .overrideProvider(Database)
            .useValue({ run: runMock })
            .overrideProvider(PubSub)
            .useValue({ getTopics: getTopicsMock }),
      }),
    });
    request = supertest(app.getHttpServer());
  });

  afterEach(async () => {
    await app.close();
  });

  it('should respond to /health', async () => {
    await request.get('/health').expect(200);
  });

  it('should return 503 when Spanner is unhealthy', async () => {
    runMock.mockRejectedValueOnce(new Error('💥'));

    await request.get('/health').expect(503);
  });

  it('should return 503 when Pub/Sub is unhealthy', async () => {
    getTopicsMock.mockRejectedValueOnce(new Error('💥'));

    await request.get('/health').expect(503);
  });
});

import { HealthCheckModule, createApp } from '@causa/runtime/nestjs';
import { jest } from '@jest/globals';
import { INestApplication, Module } from '@nestjs/common';
import supertest from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
import { FirestoreAdminClient } from '../firebase/index.js';
import { FirebaseModule } from '../firebase/module.js';
import { FirestoreHealthIndicator } from './healthcheck.js';

@Module({
  imports: [
    FirebaseModule.forRoot(),
    HealthCheckModule.forIndicators([FirestoreHealthIndicator]),
  ],
})
export class HealthModule {}

describe('FirestoreHealthIndicator', () => {
  let app: INestApplication;
  let adminClient: FirestoreAdminClient;
  let request: TestAgent<supertest.Test>;

  beforeEach(async () => {
    app = await createApp(HealthModule);
    adminClient = app.get(FirestoreAdminClient);
    request = supertest(app.getHttpServer());
  });

  afterEach(async () => {
    await app.close();
  });

  it('should return 200 when Firestore can be reached', async () => {
    jest.spyOn(adminClient as any, 'getDatabase').mockResolvedValue([]);

    await request.get('/health').expect(200, {
      status: 'ok',
      info: { 'google.firestore': { status: 'up' } },
      error: {},
      details: { 'google.firestore': { status: 'up' } },
    });

    expect(adminClient.getDatabase).toHaveBeenCalledWith({
      name: `projects/${process.env.GCLOUD_PROJECT}/databases/(default)`,
    });
  });

  it('should return 503 when Firestore cannot be reached', async () => {
    jest
      .spyOn(adminClient as any, 'getDatabase')
      .mockRejectedValue(new Error('💥'));

    await request.get('/health').expect(503, {
      status: 'error',
      info: {},
      error: { 'google.firestore': { status: 'down', error: '💥' } },
      details: { 'google.firestore': { status: 'down', error: '💥' } },
    });

    expect(adminClient.getDatabase).toHaveBeenCalledWith({
      name: `projects/${process.env.GCLOUD_PROJECT}/databases/(default)`,
    });
  });
});

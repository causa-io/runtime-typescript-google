import { HealthCheckModule, createApp } from '@causa/runtime/nestjs';
import { jest } from '@jest/globals';
import { INestApplication, Module } from '@nestjs/common';
import { Firestore } from 'firebase-admin/firestore';
import supertest from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
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
  let request: TestAgent<supertest.Test>;

  beforeEach(async () => {
    app = await createApp(HealthModule);
    request = supertest(app.getHttpServer());
  });

  afterEach(async () => {
    await app.close();
  });

  it('should return 200 when Firestore can be reached', async () => {
    await request.get('/health').expect(200, {
      status: 'ok',
      info: { 'google.firestore': { status: 'up' } },
      error: {},
      details: { 'google.firestore': { status: 'up' } },
    });
  });

  it('should return 503 when Firestore cannot be reached', async () => {
    jest
      .spyOn(app.get(Firestore), 'listCollections')
      .mockRejectedValue(new Error('💥'));

    await request.get('/health').expect(503, {
      status: 'error',
      info: {},
      error: { 'google.firestore': { status: 'down', error: '💥' } },
      details: { 'google.firestore': { status: 'down', error: '💥' } },
    });
  });
});

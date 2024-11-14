import { createApp } from '@causa/runtime/nestjs';
import { getLoggedWarnings, spyOnLogger } from '@causa/runtime/testing';
import { jest } from '@jest/globals';
import { Controller, Get, type INestApplication, Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { AppCheck } from 'firebase-admin/app-check';
import 'jest-extended';
import supertest from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
import { FirebaseModule } from '../firebase/index.js';
import { AppCheckDisabled } from './app-check-disabled.decorator.js';
import { AppCheckGuard } from './guard.js';

@Controller()
export class MyController {
  @Get()
  get() {
    return '🛂';
  }

  @Get('noCheck')
  @AppCheckDisabled()
  noCheck() {
    return '👍';
  }
}

@Controller('alsoNoCheck')
@AppCheckDisabled()
export class MyNoCheckController {
  @Get()
  get() {
    return '✅';
  }
}

@Module({
  controllers: [MyController, MyNoCheckController],
  imports: [FirebaseModule.forTesting()],
  providers: [{ provide: APP_GUARD, useClass: AppCheckGuard }],
})
class MyModule {}

describe('AppCheckGuard', () => {
  let app: INestApplication;
  let request: TestAgent<supertest.Test>;

  beforeEach(async () => {
    app = await createApp(MyModule);
    request = supertest(app.getHttpServer());
    spyOnLogger();
  });

  afterEach(async () => {
    await app.close();
  });

  it('should return 401 when the request does not have an App Check token', async () => {
    await request.get('/').expect(401);
  });

  it('should return 401 when the request does not have a valid App Check token', async () => {
    const appCheck = app.get(AppCheck);
    const token = 'nope';
    jest.spyOn(appCheck, 'verifyToken').mockRejectedValueOnce(new Error('💥'));

    await request.get('/').set({ 'X-Firebase-AppCheck': token }).expect(401);

    expect(appCheck.verifyToken).toHaveBeenCalledExactlyOnceWith(token);
    expect(getLoggedWarnings()).toEqual([
      expect.objectContaining({
        error: expect.stringContaining('Error: 💥'),
        message: 'App Check token verification failed.',
      }),
    ]);
  });

  it('should return 200 when the request has a valid App Check token', async () => {
    const appCheck = app.get(AppCheck);
    const token = 'valid';
    jest.spyOn(appCheck, 'verifyToken').mockResolvedValue({} as any);

    await request
      .get('/')
      .set({ 'X-Firebase-AppCheck': token })
      .expect(200, '🛂');

    expect(appCheck.verifyToken).toHaveBeenCalledExactlyOnceWith(token);
  });

  it('should not enforce App Check on routes decorated with disabled', async () => {
    await request.get('/noCheck').expect(200, '👍');
    await request.get('/alsoNoCheck').expect(200, '✅');
  });
});

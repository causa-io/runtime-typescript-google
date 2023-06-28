import { User } from '@causa/runtime';
import { getLoggedWarnings, spyOnLogger } from '@causa/runtime/logging/testing';
import { AuthModule, AuthUser, createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { Controller, Get, INestApplication, Module } from '@nestjs/common';
import supertest from 'supertest';
import { setTimeout } from 'timers/promises';
import { FirebaseModule } from '../firebase/index.js';
import { IdentityPlatformStrategy } from './identity-platform.strategy.js';
import { AuthUsersFixture } from './testing.js';

@Controller()
class MyController {
  @Get()
  get(@AuthUser() user: User) {
    return user.id;
  }
}

@Module({
  imports: [AuthModule, FirebaseModule.forTesting()],
  controllers: [MyController],
  providers: [IdentityPlatformStrategy],
})
class MyModule {}

describe('IdentityPlatformStrategy', () => {
  let fixture: AuthUsersFixture;
  let app: INestApplication;
  let request: supertest.SuperTest<supertest.Test>;

  async function startApp(config: Record<string, string> = {}): Promise<void> {
    app = await createApp(MyModule, {
      appFactory: makeTestAppFactory({
        config: { ...process.env, ...config },
      }),
    });
    request = supertest(app.getHttpServer());
  }

  beforeEach(async () => {
    spyOnLogger();
    fixture = new AuthUsersFixture();
  });

  afterEach(async () => {
    await fixture.deleteAll();
    await app?.close();
  });

  it('should return 401 if no token is provided', async () => {
    await startApp();

    await request.get('/').expect(401);
  });

  it('should return a 401 if the token is invalid', async () => {
    await startApp();

    await request.get('/').auth('bob', { type: 'bearer' }).expect(401);

    expect(getLoggedWarnings()).toEqual([
      expect.objectContaining({
        message: 'Token verification failed.',
        error: expect.stringContaining('Decoding Firebase ID token failed.'),
      }),
    ]);
  });

  it('should return a 401 if the token is expired', async () => {
    const { token } = await fixture.createAuthUserAndToken(
      { id: 'bob' },
      { expiresIn: '1ms' },
    );
    await Promise.all([setTimeout(5), startApp()]);

    await request.get('/').auth(token, { type: 'bearer' }).expect(401);
  });

  it('should return the user when the token is valid', async () => {
    const { token } = await fixture.createAuthUserAndToken({ id: 'bob' });
    await startApp();

    await request
      .get('/')
      .auth(token, { type: 'bearer' })
      .expect(200)
      .expect('bob');
  });

  it('should not check for revoked tokens by default', async () => {
    const { token } = await fixture.createAuthUserAndToken({ id: 'bob' });
    // For some reason:
    // - The `firebase-admin` package automatically checks for revoked tokens when using the emulator.
    // - But, revoking the tokens does not seem to work with the emulator.
    // - However, the `verifyIdToken` method also checks for the user being disabled.
    // This means this test does not actually test the `checkRevoked` option, but it is left as a placeholder if the
    // behavior of the `firebase-admin` SDK or emulator changes.
    await fixture.auth.revokeRefreshTokens('bob');
    await startApp();

    await request
      .get('/')
      .auth(token, { type: 'bearer' })
      .expect(200)
      .expect('bob');
  });

  it('should return a 401 if the token is revoked or the user is disabled', async () => {
    const { token } = await fixture.createAuthUserAndToken({ id: 'bob' });
    // Token revocation does not seem to work, but that might be due to the emulator.
    // Because `verifyIdToken` also checks for the user being disabled, this does the trick.
    await fixture.auth.updateUser('bob', { disabled: true });
    await startApp({ IDENTITY_PLATFORM_STRATEGY_CHECK_REVOKED_TOKEN: 'true' });

    await request.get('/').auth(token, { type: 'bearer' }).expect(401);
  });
});

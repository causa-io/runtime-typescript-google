import type { User } from '@causa/runtime';
import { AuthModule, AuthUser } from '@causa/runtime/nestjs';
import { AppFixture, LoggingFixture } from '@causa/runtime/nestjs/testing';
import { Controller, Get, Module } from '@nestjs/common';
import { setTimeout } from 'timers/promises';
import { FirebaseModule } from '../firebase/index.js';
import { FirebaseFixture } from '../testing.js';
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
  let appFixture: AppFixture;
  let fixture: AuthUsersFixture;

  async function startApp(config: Record<string, string> = {}): Promise<void> {
    fixture = new AuthUsersFixture();
    appFixture = new AppFixture(MyModule, {
      fixtures: [new FirebaseFixture(), fixture],
      config: { ...process.env, ...config },
    });
    await appFixture.init();
  }

  afterEach(() => appFixture.delete());

  it('should return 401 if no token is provided', async () => {
    await startApp();

    await appFixture.request.get('/').expect(401);
  });

  it('should return a 401 if the token is invalid', async () => {
    await startApp();

    await appFixture.request
      .get('/')
      .auth('bob', { type: 'bearer' })
      .expect(401);

    appFixture.get(LoggingFixture).expectWarnings({
      message: 'Token verification failed.',
      error: expect.stringContaining('Decoding Firebase ID token failed.'),
    });
  });

  it('should return a 401 if the token is expired', async () => {
    await startApp();
    const { token } = await fixture.createAuthUserAndToken(
      { id: 'bob' },
      { expiresIn: '1ms' },
    );
    await setTimeout(5);

    await appFixture.request
      .get('/')
      .auth(token, { type: 'bearer' })
      .expect(401);
  });

  it('should return the user when the token is valid', async () => {
    await startApp();
    const { token } = await fixture.createAuthUserAndToken({ id: 'bob' });

    await appFixture.request
      .get('/')
      .auth(token, { type: 'bearer' })
      .expect(200)
      .expect('bob');
  });

  it('should not check for revoked tokens by default', async () => {
    await startApp();
    const { token } = await fixture.createAuthUserAndToken({ id: 'bob' });
    // For some reason:
    // - The `firebase-admin` package automatically checks for revoked tokens when using the emulator.
    // - But, revoking the tokens does not seem to work with the emulator.
    // - However, the `verifyIdToken` method also checks for the user being disabled.
    // This means this test does not actually test the `checkRevoked` option, but it is left as a placeholder if the
    // behavior of the `firebase-admin` SDK or emulator changes.
    await fixture.auth.revokeRefreshTokens('bob');

    await appFixture.request
      .get('/')
      .auth(token, { type: 'bearer' })
      .expect(200)
      .expect('bob');
  });

  it('should return a 401 if the token is revoked or the user is disabled', async () => {
    await startApp({ IDENTITY_PLATFORM_STRATEGY_CHECK_REVOKED_TOKEN: 'true' });
    const { token } = await fixture.createAuthUserAndToken({ id: 'bob' });
    // Token revocation does not seem to work, but that might be due to the emulator.
    // Because `verifyIdToken` also checks for the user being disabled, this does the trick.
    await fixture.auth.updateUser('bob', { disabled: true });

    await appFixture.request
      .get('/')
      .auth(token, { type: 'bearer' })
      .expect(401);
  });
});

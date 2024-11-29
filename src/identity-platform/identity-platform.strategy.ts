import type { User } from '@causa/runtime';
import { Logger, UnauthenticatedError } from '@causa/runtime/nestjs';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { PassportStrategy } from '@nestjs/passport';
import { Auth, type DecodedIdToken } from 'firebase-admin/auth';
import { Strategy } from 'passport-http-bearer';

/**
 * A Passport strategy that verifies a bearer token using Google Identity Platform.
 *
 * The `IDENTITY_PLATFORM_STRATEGY_CHECK_REVOKED_TOKEN` configuration key can be set to `true` to also check whether the
 * token was revoked. This implies a call to the Identity Platform, which will increase latency.
 */
@Injectable()
export class IdentityPlatformStrategy extends PassportStrategy(Strategy) {
  /**
   * Whether to check for revoked tokens when validating them.
   */
  private readonly checkRevoked: boolean;

  constructor(
    private readonly auth: Auth,
    private readonly configService: ConfigService,
    private readonly logger: Logger,
  ) {
    super();

    this.logger.setContext(IdentityPlatformStrategy.name);
    this.checkRevoked = this.configService.get<boolean>(
      'IDENTITY_PLATFORM_STRATEGY_CHECK_REVOKED_TOKEN',
      false,
    );
  }

  async validate(token: string): Promise<User> {
    let decodedToken!: DecodedIdToken;
    try {
      decodedToken = await this.auth.verifyIdToken(token, this.checkRevoked);
    } catch (error: any) {
      if (error.code === 'auth/id-token-expired') {
        this.logger.info('Received expired token.');
      } else {
        this.logger.warn({ error: error.stack }, 'Token verification failed.');
      }

      throw new UnauthenticatedError();
    }

    const user = {
      ...decodedToken,
      id: decodedToken.uid,
    } as User;
    delete user.uid;

    return user;
  }
}

import { Logger, UnauthenticatedError } from '@causa/runtime/nestjs';
import { CanActivate, ExecutionContext, Injectable } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { Request } from 'express';
import { AppCheck } from 'firebase-admin/app-check';
import { APP_CHECK_DISABLED_METADATA_KEY } from './app-check-disabled.decorator.js';

/**
 * A NestJS guard that verifies the App Check token in the request.
 * The token is expected to be in the `X-Firebase-AppCheck` header.
 */
@Injectable()
export class AppCheckGuard implements CanActivate {
  constructor(
    private readonly appCheck: AppCheck,
    private readonly reflector: Reflector,
    private readonly logger: Logger,
  ) {}

  async canActivate(context: ExecutionContext): Promise<boolean> {
    const isDisabled = this.reflector.getAllAndOverride<boolean>(
      APP_CHECK_DISABLED_METADATA_KEY,
      [context.getHandler(), context.getClass()],
    );
    if (isDisabled) {
      return true;
    }

    const request = context.switchToHttp().getRequest<Request>();

    const appCheckToken = request.headers['x-firebase-appcheck'];
    if (!appCheckToken || typeof appCheckToken !== 'string') {
      throw new UnauthenticatedError();
    }

    try {
      await this.appCheck.verifyToken(appCheckToken);
      return true;
    } catch (error: any) {
      this.logger.warn(
        { error: error.stack },
        'App Check token verification failed.',
      );

      throw new UnauthenticatedError();
    }
  }
}

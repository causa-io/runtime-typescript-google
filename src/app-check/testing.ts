import { NestJsModuleOverrider } from '@causa/runtime/nestjs/testing';
import { CanActivate } from '@nestjs/common';
import { AppCheckGuard } from './guard.js';

/**
 * A {@link NestJsModuleOverrider} that disables the {@link AppCheckGuard}.
 *
 * If used as an app-level guard, the {@link AppCheckGuard} should be defined as a provider first, and the app guard
 * should reuse the existing instance:
 *
 * ```typescript
 * { provide: APP_GUARD, useExisting: AppCheckGuard }
 * ```
 */
export const overrideAppCheck: NestJsModuleOverrider = (builder) =>
  builder.overrideProvider(AppCheckGuard).useValue({
    canActivate() {
      return true;
    },
  } as CanActivate);

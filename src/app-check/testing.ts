import type {
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import type { CanActivate } from '@nestjs/common';
import { AppCheckGuard } from './guard.js';

/**
 * A {@link Fixture} that disables the {@link AppCheckGuard}.
 *
 * If used as an app-level guard, the {@link AppCheckGuard} should be defined as a provider first, and the app guard
 * should reuse the existing instance:
 *
 * ```typescript
 * { provide: APP_GUARD, useExisting: AppCheckGuard }
 * ```
 */
export class AppCheckFixture implements Fixture {
  async init(): Promise<NestJsModuleOverrider> {
    const mockGuard: CanActivate = { canActivate: () => true };
    return (builder) =>
      builder.overrideProvider(AppCheckGuard).useValue(mockGuard);
  }

  async clear(): Promise<void> {}

  async delete(): Promise<void> {}
}

import type {
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import { getDefaultFirebaseApp } from './app.js';
import { FIREBASE_APP_TOKEN } from './inject-firebase-app.decorator.js';
import { FirebaseLifecycleService } from './lifecycle.service.js';

/**
 * A {@link Fixture} that reuses the default Firebase application and prevents its deletion upon shutdown.
 */
export class FirebaseFixture implements Fixture {
  async init(): Promise<NestJsModuleOverrider> {
    return (builder) =>
      builder
        .overrideProvider(FIREBASE_APP_TOKEN)
        .useValue(getDefaultFirebaseApp())
        .overrideProvider(FirebaseLifecycleService)
        .useValue({});
  }

  async clear(): Promise<void> {}

  async delete(): Promise<void> {}
}

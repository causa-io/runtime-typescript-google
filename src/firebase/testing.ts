import { NestJsModuleOverrider } from '@causa/runtime/nestjs/testing';
import { getDefaultFirebaseApp } from './app.js';
import { FIREBASE_APP_TOKEN } from './inject-firebase-app.decorator.js';
import { FirebaseLifecycleService } from './lifecycle.service.js';

/**
 * A {@link NestJsModuleOverrider} that reuses {@link getDefaultFirebaseApp} and prevents the deletion of the Firebase
 * application upon shutdown.
 */
export const overrideFirebaseApp: NestJsModuleOverrider = (builder) =>
  builder
    .overrideProvider(FIREBASE_APP_TOKEN)
    .useValue(getDefaultFirebaseApp())
    .overrideProvider(FirebaseLifecycleService)
    .useValue({});

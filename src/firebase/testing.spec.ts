import { createApp } from '@causa/runtime/nestjs';
import { makeTestAppFactory } from '@causa/runtime/nestjs/testing';
import { INestApplication, Module } from '@nestjs/common';
import { App } from 'firebase-admin/app';
import { getDefaultFirebaseApp } from './app.js';
import { FIREBASE_APP_TOKEN } from './inject-firebase-app.decorator.js';
import { FirebaseModule } from './module.js';
import { overrideFirebaseApp } from './testing.js';

describe('testing', () => {
  describe('overrideFirebaseApp', () => {
    it('should override a custom Firebase App with the default app', async () => {
      @Module({ imports: [FirebaseModule.forRoot()] })
      class MyModule {}

      let app: INestApplication | undefined;
      let actualFirebaseApp: App;
      try {
        app = await createApp(MyModule, {
          appFactory: makeTestAppFactory({
            overrides: overrideFirebaseApp,
          }),
        });
        actualFirebaseApp = app.get(FIREBASE_APP_TOKEN);
      } finally {
        await app?.close();
      }

      // If `FirebaseLifecycleService` is not overridden, this throws because `app.close()` has deleted the app.
      const expectedApp = getDefaultFirebaseApp();
      expect(actualFirebaseApp).toBe(expectedApp);
    });
  });
});

import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Module } from '@nestjs/common';
import type { App } from 'firebase-admin/app';
import { getDefaultFirebaseApp } from './app.js';
import { FIREBASE_APP_TOKEN } from './inject-firebase-app.decorator.js';
import { FirebaseModule } from './module.js';
import { FirebaseFixture } from './testing.js';

@Module({ imports: [FirebaseModule.forRoot()] })
class MyModule {}

describe('FirebaseFixture', () => {
  it('should override a custom Firebase App with the default app', async () => {
    const appFixture = new AppFixture(MyModule, {
      fixtures: [new FirebaseFixture()],
    });
    await appFixture.init();

    let actualFirebaseApp: App;
    try {
      actualFirebaseApp = appFixture.app.get(FIREBASE_APP_TOKEN);
    } finally {
      await appFixture.delete();
    }

    // If `FirebaseLifecycleService` is not overridden, this throws because `app.close()` has deleted the app.
    const expectedApp = getDefaultFirebaseApp();
    expect(actualFirebaseApp).toBe(expectedApp);
  });
});

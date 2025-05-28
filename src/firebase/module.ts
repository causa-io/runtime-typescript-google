import {
  Module,
  type ClassProvider,
  type DynamicModule,
  type FactoryProvider,
  type ModuleMetadata,
  type Provider,
  type ValueProvider,
} from '@nestjs/common';
import { initializeApp, type App, type AppOptions } from 'firebase-admin/app';
import { AppCheck, getAppCheck } from 'firebase-admin/app-check';
import { Auth, getAuth } from 'firebase-admin/auth';
import {
  Firestore,
  getFirestore,
  type Settings,
} from 'firebase-admin/firestore';
import { getMessaging, Messaging } from 'firebase-admin/messaging';
import { getDefaultFirebaseApp } from './app.js';
import { FirestoreAdminClient } from './firestore-admin-client.type.js';
import { FIREBASE_APP_TOKEN } from './inject-firebase-app.decorator.js';
import { FirebaseLifecycleService } from './lifecycle.service.js';

/**
 * The NestJS injection token for Firestore settings.
 */
const FIRESTORE_SETTINGS_TOKEN = 'CAUSA_FIRESTORE_SETTINGS';

/**
 * The default Firestore settings to use when initializing the Firestore client.
 */
const DEFAULT_FIRESTORE_SETTINGS: Settings = {
  ignoreUndefinedProperties: true,
};

/**
 * The providers for service-specific Firebase clients.
 * Options for the services can be passed using injection tokens.
 */
const childProviders: (
  | ClassProvider<any>
  | ValueProvider<any>
  | FactoryProvider<any>
)[] = [
  { provide: Auth, useFactory: getAuth, inject: [FIREBASE_APP_TOKEN] },
  {
    provide: Firestore,
    useFactory: (app: App, settings: Settings) => {
      const firestore = getFirestore(app);

      try {
        // Firestore settings can only be set once, but we cannot know if they've already been set without using private
        // APIs. Calling this several times could occur in testing, when the default app is reused.
        firestore.settings(settings);
        return firestore;
      } catch (error) {
        // The Firestore SDK does not type the error more precisely.
        if (
          error instanceof Error &&
          error.message.includes('Firestore has already been initialized.')
        ) {
          return firestore;
        }

        throw error;
      }
    },
    inject: [FIREBASE_APP_TOKEN, FIRESTORE_SETTINGS_TOKEN],
  },
  { provide: AppCheck, useFactory: getAppCheck, inject: [FIREBASE_APP_TOKEN] },
  {
    provide: Messaging,
    useFactory: getMessaging,
    inject: [FIREBASE_APP_TOKEN],
  },
  {
    provide: FirestoreAdminClient,
    useFactory: () => new FirestoreAdminClient(),
  },
];

/**
 * Options for the various Firebase services (other than the base Firebase app).
 */
export type FirebaseModuleServiceOptions = {
  /**
   * Options for the Firestore client.
   * The default configuration sets {@link Settings.ignoreUndefinedProperties} to `true`.
   */
  firestore?: Settings;
};

/**
 * Options when configuring the {@link FirebaseModule}.
 */
export type FirebaseModuleOptions = AppOptions &
  FirebaseModuleServiceOptions & {
    /**
     * The name of the Firebase app to initialize.
     */
    appName?: string;
  };

/**
 * Creates the module metadata for the {@link FirebaseModule}.
 *
 * @param useDefaultFactory Whether to use {@link getDefaultFirebaseApp} to (re)use the default Firebase app.
 * @param options Options when configuring the {@link FirebaseModule}.
 *   If the default Firebase app is used, app options are ignored.
 * @returns The module metadata.
 */
function createModuleMetadata(
  useDefaultFactory: boolean,
  options: FirebaseModuleOptions,
): ModuleMetadata {
  const { appName, firestore, ...appOptions } = options;

  const appFactory = useDefaultFactory
    ? getDefaultFirebaseApp
    : () => initializeApp(appOptions, appName);

  const providers: Provider[] = [
    { provide: FIREBASE_APP_TOKEN, useFactory: appFactory },
    {
      provide: FIRESTORE_SETTINGS_TOKEN,
      useValue: { ...DEFAULT_FIRESTORE_SETTINGS, ...firestore },
    },
    ...childProviders,
  ];
  if (!useDefaultFactory) {
    providers.push(FirebaseLifecycleService);
  }

  return {
    providers,
    exports: [FIREBASE_APP_TOKEN, ...childProviders.map((p) => p.provide)],
  };
}

/**
 * A NestJS module that exports providers for the Firebase `App` and service-specific clients.
 *
 * The Firebase `App` can be injected using the {@link FIREBASE_APP_TOKEN} token.
 * {@link Auth}, {@link Firestore}, and {@link AppCheck} can be injected using their respective classes.
 */
@Module(createModuleMetadata(false, {}))
export class FirebaseModule {
  /**
   * Creates a global NestJS module that exports providers for the Firebase `App` and service-specific clients.
   *
   * @param options Options when configuring the Firebase application.
   * @returns The module.
   */
  static forRoot(options: FirebaseModuleOptions = {}): DynamicModule {
    return {
      ...createModuleMetadata(false, options),
      module: FirebaseModule,
      global: true,
    };
  }

  /**
   * Creates a NestJS module that exports providers for the Firebase `App` and service-specific clients.
   *
   * @param options Options when configuring the Firebase application.
   * @returns The module.
   */
  static register(options: FirebaseModuleOptions = {}): DynamicModule {
    return {
      ...createModuleMetadata(false, options),
      module: FirebaseModule,
    };
  }

  /**
   * Creates a global NestJS module that exports providers for the Firebase `App` and service-specific clients.
   * The default Firebase app will be (re)used using {@link getDefaultFirebaseApp}, and no options will be passed to
   * {@link initializeApp}.
   * When testing, this avoids repeatedly initializing the same app, which would result in an error.
   *
   * @param options Options for the Firebase services (other than the base Firebase app).
   * @returns The module.
   */
  static forTesting(options: FirebaseModuleServiceOptions = {}): DynamicModule {
    return {
      ...createModuleMetadata(true, options),
      module: FirebaseModule,
      global: true,
    };
  }
}

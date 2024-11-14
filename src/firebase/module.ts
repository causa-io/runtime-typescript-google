import {
  type ClassProvider,
  type DynamicModule,
  type FactoryProvider,
  Module,
  type ModuleMetadata,
  type Provider,
  type ValueProvider,
} from '@nestjs/common';
import { type AppOptions, initializeApp } from 'firebase-admin/app';
import { AppCheck, getAppCheck } from 'firebase-admin/app-check';
import { Auth, getAuth } from 'firebase-admin/auth';
import { Firestore, getFirestore } from 'firebase-admin/firestore';
import { Messaging, getMessaging } from 'firebase-admin/messaging';
import { getDefaultFirebaseApp } from './app.js';
import { FirestoreAdminClient } from './firestore-admin-client.type.js';
import { FIREBASE_APP_TOKEN } from './inject-firebase-app.decorator.js';
import { FirebaseLifecycleService } from './lifecycle.service.js';

/**
 * The providers for service-specific Firebase clients.
 * Those do not have any options as they inherit them from the `App`.
 */
const childProviders: (
  | ClassProvider<any>
  | ValueProvider<any>
  | FactoryProvider<any>
)[] = [
  { provide: Auth, useFactory: getAuth, inject: [FIREBASE_APP_TOKEN] },
  {
    provide: Firestore,
    useFactory: getFirestore,
    inject: [FIREBASE_APP_TOKEN],
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
 * Options when configuring the {@link FirebaseModule}.
 */
export type FirebaseModuleOptions = AppOptions & {
  /**
   * The name of the Firebase app to initialize.
   */
  appName?: string;
};

/**
 * Creates the module metadata for the {@link FirebaseModule}.
 *
 * @param options Options when configuring the {@link FirebaseModule}.
 *   If set to `default`, the default Firebase app will be (re)used using {@link getDefaultFirebaseApp}.
 * @returns The module metadata.
 */
function createModuleMetadata(
  options: FirebaseModuleOptions | 'default' = {},
): ModuleMetadata {
  const useDefaultFactory = options === 'default';
  const { appName, ...appOptions } = useDefaultFactory
    ? ({} as FirebaseModuleOptions)
    : options;

  const appFactory = useDefaultFactory
    ? getDefaultFirebaseApp
    : () => initializeApp(appOptions, appName);

  const providers: Provider[] = [
    { provide: FIREBASE_APP_TOKEN, useFactory: appFactory },
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
@Module(createModuleMetadata())
export class FirebaseModule {
  /**
   * Creates a global NestJS module that exports providers for the Firebase `App` and service-specific clients.
   *
   * @param options Options when configuring the Firebase application.
   * @returns The module.
   */
  static forRoot(options: FirebaseModuleOptions = {}): DynamicModule {
    return {
      ...createModuleMetadata(options),
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
      ...createModuleMetadata(options),
      module: FirebaseModule,
    };
  }

  /**
   * Creates a global NestJS module that exports providers for the Firebase `App` and service-specific clients.
   * The default Firebase app will be (re)used using {@link getDefaultFirebaseApp}, and no options will be passed to
   * {@link initializeApp}.
   * When testing, this avoids repeatedly initializing the same app, which would result in an error.
   *
   * @returns The module.
   */
  static forTesting(): DynamicModule {
    return {
      ...createModuleMetadata('default'),
      module: FirebaseModule,
      global: true,
    };
  }
}

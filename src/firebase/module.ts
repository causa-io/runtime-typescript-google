import {
  ClassProvider,
  DynamicModule,
  FactoryProvider,
  Module,
  ModuleMetadata,
  ValueProvider,
} from '@nestjs/common';
import { AppOptions, initializeApp } from 'firebase-admin/app';
import { AppCheck, getAppCheck } from 'firebase-admin/app-check';
import { Auth, getAuth } from 'firebase-admin/auth';
import { Firestore, getFirestore } from 'firebase-admin/firestore';
import { getDefaultFirebaseApp } from './app.js';

/**
 * The NestJS token used to inject the Firebase `App`.
 */
export const FIREBASE_APP_TOKEN = 'CAUSA_FIREBASE';

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

  return {
    providers: [
      { provide: FIREBASE_APP_TOKEN, useFactory: appFactory },
      ...childProviders,
    ],
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

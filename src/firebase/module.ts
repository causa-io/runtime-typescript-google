import {
  ClassProvider,
  DynamicModule,
  FactoryProvider,
  Module,
  ModuleMetadata,
  ValueProvider,
} from '@nestjs/common';
import { AppOptions, initializeApp } from 'firebase-admin/app';
import { Auth, getAuth } from 'firebase-admin/auth';
import { Firestore, getFirestore } from 'firebase-admin/firestore';

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
  {
    provide: Auth,
    useFactory: getAuth,
    inject: [FIREBASE_APP_TOKEN],
  },
  {
    provide: Firestore,
    useFactory: getFirestore,
    inject: [FIREBASE_APP_TOKEN],
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
 * @returns The module metadata.
 */
function createModuleMetadata(
  options: FirebaseModuleOptions = {},
): ModuleMetadata {
  const { appName, ...appOptions } = options;

  return {
    providers: [
      {
        provide: FIREBASE_APP_TOKEN,
        useFactory: () => initializeApp(appOptions, appName),
      },
      ...childProviders,
    ],
    exports: [FIREBASE_APP_TOKEN, ...childProviders.map((p) => p.provide)],
  };
}

/**
 * A NestJS module that exports providers for the Firebase `App` and service-specific clients.
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
}

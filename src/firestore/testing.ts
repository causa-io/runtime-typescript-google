import type {
  AppFixture,
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import type { Type } from '@nestjs/common';
import type { App } from 'firebase-admin/app';
import {
  CollectionReference,
  Firestore,
  type Settings,
} from 'firebase-admin/firestore';
import { randomUUID } from 'node:crypto';
import { FIREBASE_APP_TOKEN } from '../firebase/index.js';
import {
  FIRESTORE_SETTINGS_TOKEN,
  createFirestore,
} from '../firebase/module.js';
import { getFirestoreCollection } from './collection.decorator.js';

/**
 * Clears all the documents in the database used by the given {@link Firestore} instance.
 *
 * @param firestore The {@link Firestore} instance for which the database should be cleared.
 */
export async function clearFirestoreDatabase(
  firestore: Firestore,
): Promise<void> {
  const host = process.env.FIRESTORE_EMULATOR_HOST;
  if (!host) {
    throw new Error(
      'The FIRESTORE_EMULATOR_HOST environment variable must be set to clear a Firestore database.',
    );
  }

  const projectId =
    process.env.GOOGLE_CLOUD_PROJECT ?? process.env.GCLOUD_PROJECT;
  if (!projectId) {
    throw new Error(
      'The GOOGLE_CLOUD_PROJECT environment variable must be set to clear a Firestore database.',
    );
  }

  const response = await fetch(
    `http://${host}/emulator/v1/projects/${projectId}/databases/${firestore.databaseId}/documents`,
    { method: 'DELETE' },
  );
  if (!response.ok) {
    throw new Error(
      `Failed to clear the Firestore database: ${response.status} ${await response.text()}.`,
    );
  }
}

/**
 * A {@link Fixture} that makes the application use a separate (random) Firestore database, and clears it when
 * requested.
 */
export class FirestoreFixture implements Fixture {
  /**
   * The parent {@link AppFixture}.
   */
  private appFixture!: AppFixture;

  /**
   * The ID of the Firestore database used during tests.
   */
  readonly databaseId: string;

  constructor(
    options: {
      /**
       * The ID of the Firestore database to use. Defaults to a random ID.
       */
      databaseId?: string;
    } = {},
  ) {
    this.databaseId = options.databaseId ?? `test-${randomUUID()}`;
  }

  async init(appFixture: AppFixture): Promise<NestJsModuleOverrider> {
    this.appFixture = appFixture;

    return (builder) =>
      builder.overrideProvider(Firestore).useFactory({
        factory: (app: App, settings: Settings) => {
          const firestore = createFirestore(app, {
            ...settings,
            databaseId: this.databaseId,
          });
          // The `FirebaseLifecycleService` is disabled by the `FirebaseFixture`, such that the shared Firebase app is
          // not deleted. The instance for the test database is however not shared and should be terminated.
          return Object.assign(firestore, {
            onApplicationShutdown: () => firestore.terminate(),
          });
        },
        inject: [FIREBASE_APP_TOKEN, FIRESTORE_SETTINGS_TOKEN],
      });
  }

  async clear(): Promise<void> {
    await clearFirestoreDatabase(this.firestore);
  }

  async delete(): Promise<void> {
    this.appFixture = undefined as any;
  }

  /**
   * The underlying {@link Firestore} instance used by this fixture.
   */
  get firestore(): Firestore {
    return this.appFixture.get(Firestore);
  }

  /**
   * Returns the collection for the given document type, in the test database.
   *
   * @param documentType The type of the document.
   * @param document The (partial) document used to compute the collection path.
   *   This can be omitted for documents stored in root collections.
   * @returns The {@link CollectionReference} for the given document type.
   */
  collection<T>(
    documentType: Type<T>,
    document?: Partial<T>,
  ): CollectionReference<T> {
    return getFirestoreCollection(this.firestore, documentType, document);
  }
}

import type {
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
   * The ID of the Firestore database used during tests.
   */
  readonly databaseId: string;

  /**
   * The {@link Firestore} instance for the test database.
   */
  private testFirestore: Firestore | undefined;

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

  async init(): Promise<NestJsModuleOverrider> {
    return (builder) =>
      builder.overrideProvider(Firestore).useFactory({
        factory: (app: App, settings: Settings) =>
          this.getOrCreateFirestore(app, settings),
        inject: [FIREBASE_APP_TOKEN, FIRESTORE_SETTINGS_TOKEN],
      });
  }

  /**
   * Creates the {@link Firestore} instance for the test database if it does not exist yet.
   *
   * @param app The Firebase application for which the instance should be created.
   * @param settings The Firestore settings to use.
   * @returns The {@link Firestore} instance for the test database.
   */
  private getOrCreateFirestore(app: App, settings: Settings): Firestore {
    this.testFirestore ??= createFirestore(app, {
      ...settings,
      databaseId: this.databaseId,
    });

    return this.testFirestore;
  }

  async clear(): Promise<void> {
    if (!this.testFirestore) {
      return;
    }

    await clearFirestoreDatabase(this.testFirestore);
  }

  async delete(): Promise<void> {
    // The `FirebaseLifecycleService` is disabled by the `FirebaseFixture`, such that the shared Firebase app is not
    // deleted. The instance for the test database is however not shared and should be terminated.
    await this.testFirestore?.terminate();
    this.testFirestore = undefined;
  }

  /**
   * The underlying {@link Firestore} instance used by this fixture.
   */
  get firestore(): Firestore {
    if (!this.testFirestore) {
      throw new Error(
        'The Firestore instance is not available because the application does not provide one.',
      );
    }

    return this.testFirestore;
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

import type { VersionedEntity } from '@causa/runtime';
import { createApp } from '@causa/runtime/nestjs';
import {
  type MakeTestAppFactoryOptions,
  makeTestAppFactory,
} from '@causa/runtime/nestjs/testing';
import { serializeAsJavaScriptObject } from '@causa/runtime/testing';
import { Database, Spanner } from '@google-cloud/spanner';
import type { INestApplication, Type } from '@nestjs/common';
import { CollectionReference } from 'firebase-admin/firestore';
import supertest, { Test } from 'supertest';
import TestAgent from 'supertest/lib/agent.js';
import { overrideAppCheck } from '../app-check/testing.js';
import { overrideFirebaseApp } from '../firebase/testing.js';
import {
  clearFirestoreCollection,
  getFirestoreCollectionFromModule,
  overrideFirestoreCollections,
} from '../firestore/testing.js';
import { AuthUsersFixture } from '../identity-platform/testing.js';
import {
  type EventRequester,
  PubSubFixture,
  makePubSubRequester,
} from '../pubsub/testing/index.js';
import { SpannerEntityManager, type SpannerKey } from '../spanner/index.js';
import { createDatabase, overrideDatabase } from '../spanner/testing.js';

/**
 * Describes an entity to fetch using the {@link SpannerEntityManager}.
 * Used by expect methods in {@link GoogleAppFixture}.
 */
type EntityToFetch<T extends object> = {
  /**
   * The type of entity to fetch.
   */
  type: Type<T>;

  /**
   * The ID of the entity to fetch.
   */
  id: string | SpannerKey;

  /**
   * Whether to include soft-deleted results when fetching the entity.
   * See {@link SpannerEntityManager.findOneByKeyOrFail} options.
   */
  includeSoftDeletes?: boolean;
};

/**
 * Describes tests to run on a versioned entity stored in Spanner.
 */
type VersionedEntityTests<T extends object> = {
  /**
   * The expected entity after mutation.
   * It is checked against the actual entity in the database using `toEqual`, which means it can contain matchers.
   * A function can be provided to generate the expected entity from the actual entity.
   */
  expectedEntity: ((actual: T) => T) | T;

  /**
   * If set, checks that an event has been published to the corresponding Pub/Sub topic with the entity as `data`.
   */
  expectedEvent?: {
    /**
     * The name of the topic.
     */
    topic: string;

    /**
     * The `name` of the expected published event.
     */
    name: string;

    /**
     * Attributes expected to have been set on the published event.
     */
    attributes?: Record<string, string>;
  };

  /**
   * If set, the passed object is checked to be equal to a "plain object" (e.g. with `Date`s converted to `string`s)
   * version of the fetched entity.
   * This can contain a parsed HTTP response body which is expected to return the entity.
   *
   * {@link serializeAsJavaScriptObject} is used to serialize the fetched entity.
   */
  matchesHttpResponse?: object;
};

/**
 * A fixture for testing a NestJS application that uses Google Cloud Platform services.
 *
 * It manages the setup and teardown of resources in GCP emulators, and provides utilities for testing versioned
 * entities.
 */
export class GoogleAppFixture {
  /**
   * Creates a new {@link GoogleAppFixture} instance.
   *
   * @param app The NestJS application.
   * @param spanner The {@link Spanner} client managed by the fixture.
   * @param database The temporary Spanner database.
   * @param entityManager The {@link SpannerEntityManager} for the temporary database.
   * @param pubSub The {@link PubSubFixture} managing temporary topics.
   * @param users The {@link AuthUsersFixture}.
   * @param request The {@link SuperTest} instance for the NestJS application.
   * @param pubSubRequest The {@link EventRequester} to make requests to Pub/Sub push endpoints in the application.
   * @param entities The entities to clear from the database when calling {@link GoogleAppFixture.clear}.
   * @param firestoreDocuments The Firestore documents to clear when calling {@link GoogleAppFixture.clear}.
   */
  private constructor(
    readonly app: INestApplication,
    readonly spanner: Spanner,
    readonly database: Database,
    readonly entityManager: SpannerEntityManager,
    readonly pubSub: PubSubFixture,
    readonly users: AuthUsersFixture,
    readonly request: TestAgent<Test>,
    readonly pubSubRequest: EventRequester,
    readonly entities: Type[],
    readonly firestoreDocuments: Type[],
  ) {}

  /**
   * Returns the temporary Firestore collection (as set up by the fixture) for a given document type.
   *
   * @param document The type of document to get the collection for.
   * @returns The Firestore collection.
   */
  firestoreCollection<T>(document: Type<T>): CollectionReference<T> {
    return getFirestoreCollectionFromModule(this.app, document);
  }

  /**
   * Runs a test on a versioned entity, checking that it has been mutated as expected.
   * Optionally, checks that an event has been published to the corresponding Pub/Sub topic.
   * Also, a serialized version of the entity (e.g. an HTTP response) can be checked against the expected entity.
   *
   * @param entity Describes the entity to fetch using the {@link SpannerEntityManager}.
   * @param tests The tests to run on the entity and its event.
   * @returns The entity fetched from the database.
   */
  async expectMutatedVersionedEntity<
    T extends Pick<VersionedEntity, 'updatedAt'>,
  >(entity: EntityToFetch<T>, tests: VersionedEntityTests<T>): Promise<T> {
    const storedEntity = await this.entityManager.findOneByKeyOrFail(
      entity.type,
      entity.id,
      { includeSoftDeletes: entity.includeSoftDeletes },
    );
    const expectedEntity =
      typeof tests.expectedEntity === 'function'
        ? tests.expectedEntity(storedEntity)
        : tests.expectedEntity;
    expect(storedEntity).toEqual(expectedEntity);

    const { expectedEvent } = tests;
    if (expectedEvent) {
      await this.pubSub.expectEventInTopic(
        expectedEvent.topic,
        {
          id: expect.any(String),
          name: expectedEvent.name,
          producedAt: storedEntity.updatedAt,
          data: storedEntity,
        },
        { attributes: expectedEvent.attributes },
      );
    }

    if (tests.matchesHttpResponse) {
      const expectedResponse = await serializeAsJavaScriptObject(storedEntity);
      expect(tests.matchesHttpResponse).toEqual(expectedResponse);
    }

    return storedEntity;
  }

  /**
   * Ensures the specified entity has not been mutated.
   * Optionally, checks that no event has been published to the corresponding Pub/Sub topic.
   * Also, a serialized version of the entity (e.g. an HTTP response) can be checked against the expected entity.
   *
   * @param entity Describes the entity to fetch using the {@link SpannerEntityManager}.
   * @param tests The tests to run on the entity.
   */
  async expectNonMutatedVersionedEntity<
    T extends Pick<VersionedEntity, 'updatedAt'>,
  >(
    entity: EntityToFetch<T>,
    tests: Omit<VersionedEntityTests<T>, 'expectedEvent'> & {
      // The function is not needed here, since the entity is not mutated.
      expectedEntity: object;

      /**
       * If set, checks that no event has been published to the corresponding Pub/Sub topic.
       */
      expectNoEventInTopic?: string;
    },
  ): Promise<void> {
    const storedEntity = await this.entityManager.findOneByKeyOrFail(
      entity.type,
      entity.id,
      { includeSoftDeletes: entity.includeSoftDeletes },
    );
    expect(storedEntity).toEqual(tests.expectedEntity);

    if (tests.expectNoEventInTopic) {
      await this.pubSub.expectNoMessageInTopic(tests.expectNoEventInTopic);
    }

    if (tests.matchesHttpResponse) {
      const expectedResponse = await serializeAsJavaScriptObject(storedEntity);
      expect(tests.matchesHttpResponse).toEqual(expectedResponse);
    }
  }

  /**
   * Clears all entities from the test database.
   *
   * The tables to clear should be specified in the `entities` property of the options passed to
   * {@link GoogleAppFixture.create}.
   */
  async clearSpanner(): Promise<void> {
    await this.entityManager.transaction(async (transaction) => {
      for (const entity of this.entities) {
        await this.entityManager.clear(entity, { transaction });
      }
    });
  }

  /**
   * Clears all documents from the test Firestore collections.
   *
   * The collections to clear should be specified in the `firestoreDocuments` property of the options passed to
   * {@link GoogleAppFixture.create}.
   */
  async clearFirestore(): Promise<void> {
    await Promise.all(
      this.firestoreDocuments.map((d) =>
        clearFirestoreCollection(this.firestoreCollection(d)),
      ),
    );
  }

  /**
   * Clears all entities from the test database and all documents from the test Firestore collections.
   * Also clears all messages read from the test Pub/Sub topics up to now.
   *
   * This does **not** delete Identity Platform users, which are only deleted when calling {
   * @link GoogleAppFixture.delete}.
   */
  async clear(): Promise<void> {
    this.pubSub.clear();
    await Promise.all([this.clearSpanner(), this.clearFirestore()]);
  }

  /**
   * Deletes all resources created by the fixture.
   */
  async delete(): Promise<void> {
    await this.app.close();

    await Promise.all([
      this.users.deleteAll(),
      this.pubSub.deleteAll(),
      this.database.delete(),
    ]);

    this.spanner.close();
  }

  /**
   * Creates a NestJS application using the specified module, and sets up the fixture.
   *
   * @param appModule The NestJS module to create the application from.
   * @param options Options when creating the GCP resources for the fixture.
   * @returns The {@link GoogleAppFixture}.
   */
  static async create(
    appModule: any,
    options: {
      /**
       * Temporary Pub/Sub topics to create using the {@link PubSubFixture}.
       */
      pubSubTopics?: Record<string, Type>;

      /**
       * Temporary Firestore collections to create and to clear during teardown.
       */
      firestoreDocuments?: Type[];

      /**
       * Spanner entities to clear during teardown.
       */
      entities?: Type[];

      /**
       * Options for the {@link makeTestAppFactory} function.
       */
      appFactoryOptions?: MakeTestAppFactoryOptions;

      /**
       * Whether the `AppCheckGuard` should be disabled.
       * Defaults to `true`.
       */
      disableAppCheck?: boolean;
    } = {},
  ): Promise<GoogleAppFixture> {
    const entities = options.entities ?? [];
    const firestoreDocuments = options.firestoreDocuments ?? [];
    const disableAppCheck = options.disableAppCheck ?? true;

    const spanner = new Spanner();
    const database = await createDatabase({ spanner });
    const entityManager = new SpannerEntityManager(database);

    const pubSubFixture = new PubSubFixture();
    const overridePubSub = await pubSubFixture.createWithOverrider(
      options.pubSubTopics ?? {},
    );

    const usersFixture = new AuthUsersFixture();

    const appFactoryOptions = options.appFactoryOptions ?? {};
    const additionalOverrides =
      appFactoryOptions.overrides && !Array.isArray(appFactoryOptions.overrides)
        ? [appFactoryOptions.overrides]
        : (appFactoryOptions.overrides ?? []);
    const app = await createApp(appModule, {
      appFactory: makeTestAppFactory({
        ...appFactoryOptions,
        overrides: [
          overrideDatabase(database),
          overridePubSub,
          overrideFirebaseApp,
          overrideFirestoreCollections(...firestoreDocuments),
          ...(disableAppCheck ? [overrideAppCheck] : []),
          ...additionalOverrides,
        ],
      }),
    });

    const request = supertest(app.getHttpServer());
    const pubSubRequest = makePubSubRequester(app);

    return new GoogleAppFixture(
      app,
      spanner,
      database,
      entityManager,
      pubSubFixture,
      usersFixture,
      request,
      pubSubRequest,
      entities,
      firestoreDocuments,
    );
  }
}

import {
  Event,
  EventPublisher,
  IsDateType,
  IsNullable,
  ValidateNestedType,
  VersionedEntity,
} from '@causa/runtime';
import { EVENT_PUBLISHER_INJECTION_NAME } from '@causa/runtime/nestjs';
import { serializeAsJavaScriptObject } from '@causa/runtime/testing';
import { Database, Spanner } from '@google-cloud/spanner';
import { jest } from '@jest/globals';
import { Injectable, Module } from '@nestjs/common';
import { IsString, IsUUID } from 'class-validator';
import 'jest-extended';
import * as uuid from 'uuid';
import { FirebaseModule } from '../firebase/index.js';
import {
  FirestoreCollection,
  FirestoreCollectionsModule,
} from '../firestore/index.js';
import { PubSubPublisherModule } from '../pubsub/index.js';
import {
  SpannerColumn,
  SpannerModule,
  SpannerTable,
} from '../spanner/index.js';
import { SoftDeletedFirestoreCollection } from '../transaction/index.js';
import { GoogleAppFixture } from './google-app-fixture.js';

@SpannerTable({ primaryKey: ['id'] })
class MyEntity implements VersionedEntity {
  constructor(data: Partial<MyEntity> = {}) {
    Object.assign(this, {
      id: uuid.v4(),
      createdAt: new Date(),
      updatedAt: new Date(),
      deletedAt: null,
      someProp: 'hello',
      ...data,
    });
  }

  @IsUUID()
  @SpannerColumn()
  readonly id!: string;

  @IsDateType()
  @SpannerColumn()
  readonly createdAt!: Date;

  @IsDateType()
  @SpannerColumn()
  readonly updatedAt!: Date;

  @IsDateType()
  @IsNullable()
  @SpannerColumn({ softDelete: true })
  readonly deletedAt!: Date | null;

  @IsString()
  @SpannerColumn()
  readonly someProp!: string;
}

@FirestoreCollection({
  name: 'myCollection',
  path: (d) => d.id,
})
@SoftDeletedFirestoreCollection()
class MyDocument implements MyEntity {
  constructor(data: Partial<MyDocument> = {}) {
    Object.assign(this, {
      id: uuid.v4(),
      createdAt: new Date(),
      updatedAt: new Date(),
      deletedAt: null,
      someProp: 'hello',
      ...data,
    });
  }

  @IsUUID()
  readonly id!: string;

  @IsDateType()
  readonly createdAt!: Date;

  @IsDateType()
  readonly updatedAt!: Date;

  @IsDateType()
  @IsNullable()
  readonly deletedAt!: Date | null;

  @IsString()
  readonly someProp!: string;
}

class MyEvent implements Event {
  constructor(data: Partial<MyEvent> = {}) {
    Object.assign(this, {
      id: uuid.v4(),
      producedAt: new Date(),
      name: 'entityCreated',
      data: new MyEntity(),
      ...data,
    });
  }

  @IsUUID()
  readonly id!: string;

  @IsDateType()
  readonly producedAt!: Date;

  @IsString()
  readonly name!: string;

  @ValidateNestedType(() => MyEntity)
  readonly data!: MyEntity;
}

@Injectable()
class MyService {
  readonly id = '❌';
}

@Injectable()
class MyTestService {
  readonly id = '✅';
}

@Injectable()
class MyServiceWithDependency {
  constructor(readonly dependency: MyService) {}
}

@Module({
  imports: [
    FirebaseModule.forRoot(),
    SpannerModule.forRoot(),
    PubSubPublisherModule.forRoot(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
  ],
  providers: [MyService, MyServiceWithDependency],
})
class MyModule {}

describe('GoogleAppFixture', () => {
  let previousEnv: NodeJS.ProcessEnv;
  let sourceDatabase: Database;

  let fixture: GoogleAppFixture;
  let publisher: EventPublisher;

  beforeAll(async () => {
    const [database, operation] = await new Spanner()
      .instance(process.env.SPANNER_INSTANCE ?? '')
      .createDatabase('test-google');
    await operation.promise();

    const [updateOperation] = await database.updateSchema(`
      CREATE TABLE MyEntity (
        id STRING(36) NOT NULL,
        createdAt TIMESTAMP NOT NULL,
        updatedAt TIMESTAMP NOT NULL,
        deletedAt TIMESTAMP,
        someProp STRING(MAX) NOT NULL,
      ) PRIMARY KEY (id)`);
    await updateOperation.promise();

    sourceDatabase = database;

    process.env.SPANNER_DATABASE = 'test-google';
  });

  beforeEach(async () => {
    fixture = await GoogleAppFixture.create(MyModule, {
      entities: [MyEntity],
      pubSubTopics: { 'my.event.v1': MyEvent },
      firestoreDocuments: [MyDocument],
      appFactoryOptions: {
        overrides: (builder) =>
          builder.overrideProvider(MyService).useClass(MyTestService),
      },
    });
    publisher = fixture.app.get(EVENT_PUBLISHER_INJECTION_NAME);
  });

  afterEach(async () => {
    await fixture.clearFirestore();
    await fixture.delete();
  });

  afterAll(async () => {
    await sourceDatabase.delete();
    process.env = previousEnv;
  });

  describe('create', () => {
    it('should create a temporary Spanner database', () => {
      expect(fixture.entityManager.database.formattedName_).not.toContain(
        'test-google',
      );
    });

    it('should create a temporary Firestore collection', () => {
      const actualCollection = fixture.firestoreCollection(MyDocument);

      // The temporary collection includes a suffix.
      expect(actualCollection.path).not.toStartWith('myCollection');
    });

    it('should set appFactoryOptions', () => {
      const actualService = fixture.app.get(MyServiceWithDependency);

      expect(actualService.dependency.id).toEqual('✅');
    });
  });

  describe('clear', () => {
    it('should clear Spanner entities', async () => {
      const myEntity = new MyEntity();
      await fixture.entityManager.insert(myEntity);

      await fixture.clear();

      const actualEntity = await fixture.entityManager.findOneByKey(
        MyEntity,
        myEntity.id,
        { includeSoftDeletes: true },
      );
      expect(actualEntity).toBeUndefined();
    });

    it('should clear Firestore documents', async () => {
      const myDocument = new MyDocument();
      await fixture
        .firestoreCollection(MyDocument)
        .doc(myDocument.id)
        .set(myDocument);

      await fixture.clear();

      const actualDocument = await fixture
        .firestoreCollection(MyDocument)
        .doc(myDocument.id)
        .get();
      expect(actualDocument.exists).toBeFalse();
    });

    it('should clear the Pub/Sub fixture', async () => {
      const myEvent = new MyEvent();
      await publisher.publish('my.event.v1', myEvent);
      await fixture.pubSub.expectEventInTopic('my.event.v1', myEvent);

      await fixture.clear();

      await fixture.pubSub.expectNoMessageInTopic('my.event.v1');
    });
  });

  describe('delete', () => {
    it('should delete resources', async () => {
      const databaseName = fixture.entityManager.database.formattedName_;
      await fixture.users.createAuthUserAndToken();

      await fixture.delete();

      const [databaseExists] = await new Spanner()
        .instance(process.env.SPANNER_INSTANCE ?? '')
        .database(databaseName, { min: 0 })
        .exists();
      expect(databaseExists).toBeFalse();
      expect(Object.keys(fixture.pubSub.fixtures)).toBeEmpty();
      expect(fixture.users.users).toBeEmpty();
    });
  });

  describe('expectMutatedVersionedEntity', () => {
    it('should throw if the expected entity does not match', async () => {
      const myEntity = new MyEntity();
      await fixture.entityManager.insert(myEntity);

      const actualPromise = fixture.expectMutatedVersionedEntity(
        { type: MyEntity, id: myEntity.id },
        { expectedEntity: { ...myEntity, someProp: '🙅' } },
      );

      await expect(actualPromise).rejects.toThrow('"someProp": "🙅"');
    });

    it('should make optional checks', async () => {
      const myEntity = new MyEntity();
      const myEvent = new MyEvent({ data: myEntity });
      await fixture.entityManager.insert(myEntity);
      await publisher.publish('my.event.v1', myEvent);
      const response = await serializeAsJavaScriptObject(myEntity);
      jest.spyOn(fixture.pubSub, 'expectEventInTopic');

      await fixture.expectMutatedVersionedEntity(
        { type: MyEntity, id: myEntity.id },
        {
          expectedEntity: myEntity,
          expectedEvent: { topic: 'my.event.v1', name: myEvent.name },
          matchesHttpResponse: response,
        },
      );

      expect(fixture.pubSub.expectEventInTopic).toHaveBeenCalledExactlyOnceWith(
        'my.event.v1',
        {
          id: expect.any(String),
          name: myEvent.name,
          producedAt: myEntity.updatedAt,
          data: myEntity,
        },
      );
    });
  });

  describe('expectNonMutatedVersionedEntity', () => {
    it('should throw if the expected entity does not match', async () => {
      const myEntity = new MyEntity();
      await fixture.entityManager.insert(myEntity);

      const actualPromise = fixture.expectNonMutatedVersionedEntity(
        { type: MyEntity, id: myEntity.id },
        { expectedEntity: { ...myEntity, someProp: '🙅' } },
      );

      await expect(actualPromise).rejects.toThrow('"someProp": "🙅"');
    });

    it('should make optional checks', async () => {
      const myEntity = new MyEntity();
      await fixture.entityManager.insert(myEntity);
      const response = await serializeAsJavaScriptObject(myEntity);
      jest.spyOn(fixture.pubSub, 'expectNoMessageInTopic');

      await fixture.expectNonMutatedVersionedEntity(
        { type: MyEntity, id: myEntity.id },
        {
          expectedEntity: myEntity,
          expectNoEventInTopic: 'my.event.v1',
          matchesHttpResponse: response,
        },
      );

      expect(
        fixture.pubSub.expectNoMessageInTopic,
      ).toHaveBeenCalledExactlyOnceWith('my.event.v1');
    });
  });
});

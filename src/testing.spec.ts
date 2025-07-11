import {
  type Event,
  type EventPublisher,
  IsDateType,
  IsNullable,
  ValidateNestedType,
  type VersionedEntity,
} from '@causa/runtime';
import { EVENT_PUBLISHER_INJECTION_NAME } from '@causa/runtime/nestjs';
import { AppFixture } from '@causa/runtime/nestjs/testing';
import { Database, Spanner } from '@google-cloud/spanner';
import { Controller, Get, Module } from '@nestjs/common';
import { APP_GUARD } from '@nestjs/core';
import { IsString, IsUUID } from 'class-validator';
import 'jest-extended';
import * as uuid from 'uuid';
import { AppCheckGuard } from './app-check/index.js';
import { FirebaseModule } from './firebase/index.js';
import {
  FirestoreCollection,
  FirestoreCollectionsModule,
} from './firestore/index.js';
import { PubSubPublisherModule } from './pubsub/index.js';
import {
  SpannerColumn,
  SpannerEntityManager,
  SpannerModule,
  SpannerTable,
} from './spanner/index.js';
import {
  AuthUsersFixture,
  createGoogleFixtures,
  FirestoreFixture,
  PubSubFixture,
} from './testing.js';
import { SoftDeletedFirestoreCollection } from './transaction/index.js';

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

@Controller()
class MyController {
  @Get()
  async get(): Promise<void> {}
}

@Module({
  imports: [
    FirebaseModule.forRoot(),
    SpannerModule.forRoot(),
    PubSubPublisherModule.forRoot(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
  ],
  providers: [
    AppCheckGuard,
    { provide: APP_GUARD, useExisting: AppCheckGuard },
  ],
  controllers: [MyController],
})
class MyModule {}

describe('GoogleAppFixture', () => {
  let previousEnv: NodeJS.ProcessEnv;
  let spanner: Spanner;
  let sourceDatabase: Database;

  let fixture!: AppFixture;
  let publisher: EventPublisher;

  beforeAll(async () => {
    spanner = new Spanner();
    const [database, operation] = await spanner
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

    previousEnv = { ...process.env };
    process.env.SPANNER_DATABASE = 'test-google';
  });

  beforeEach(async () => {
    fixture = new AppFixture(MyModule, {
      fixtures: createGoogleFixtures({
        spannerTypes: [MyEntity],
        pubSubTopics: { 'my.event.v1': MyEvent },
        firestoreTypes: [MyDocument],
      }),
    });
    await fixture.init();
    publisher = fixture.get(EVENT_PUBLISHER_INJECTION_NAME);
  });

  afterEach(async () => {
    await fixture?.clear();
    await fixture?.delete();
  });

  afterAll(async () => {
    await sourceDatabase.delete();
    spanner.close();
    process.env = previousEnv;
  });

  describe('create', () => {
    it('should create a temporary Spanner database', () => {
      const actualName = fixture.get(Database).formattedName_;

      expect(actualName).not.toContain('test-google');
    });

    it('should create a temporary Firestore collection', () => {
      const actualCollection = fixture
        .get(FirestoreFixture)
        .collection(MyDocument);

      // The temporary collection includes a suffix.
      expect(actualCollection.path).not.toStartWith('myCollection');
    });
  });

  describe('clear', () => {
    it('should clear Spanner entities', async () => {
      const myEntity = new MyEntity();
      await fixture.get(SpannerEntityManager).insert(myEntity);

      await fixture.clear();

      const actualEntity = await fixture
        .get(SpannerEntityManager)
        .findOneByKey(MyEntity, myEntity.id, { includeSoftDeletes: true });
      expect(actualEntity).toBeUndefined();
    });

    it('should clear Firestore documents', async () => {
      const myDocument = new MyDocument();
      await fixture
        .get(FirestoreFixture)
        .collection(MyDocument)
        .doc(myDocument.id)
        .set(myDocument);

      await fixture.clear();

      const actualDocument = await fixture
        .get(FirestoreFixture)
        .collection(MyDocument)
        .doc(myDocument.id)
        .get();
      expect(actualDocument.exists).toBeFalse();
    });

    it('should clear the Pub/Sub fixture', async () => {
      const myEvent = new MyEvent();
      await publisher.publish('my.event.v1', myEvent);
      await fixture.get(PubSubFixture).expectEvent('my.event.v1', myEvent);

      await fixture.clear();

      await fixture.get(PubSubFixture).expectNoMessage('my.event.v1');
    });
  });

  describe('delete', () => {
    it('should delete resources', async () => {
      const databaseName = fixture.get(Database).formattedName_;
      await fixture.get(AuthUsersFixture).createAuthUserAndToken();

      await fixture.delete();

      const spanner = new Spanner();
      const [databaseExists] = await spanner
        .instance(process.env.SPANNER_INSTANCE ?? '')
        .database(databaseName, { min: 0 })
        .exists();
      expect(databaseExists).toBeFalse();
      spanner.close();
      expect(fixture.get(PubSubFixture).topics).toBeEmptyObject();
      expect(fixture.get(AuthUsersFixture).users).toBeEmpty();
      // Avoids deleting the fixture twice.
      fixture = undefined as any;
    });
  });

  describe('AppCheck', () => {
    it('should disable the AppCheckGuard', async () => {
      await fixture.request.get('/').expect(200);
    });
  });
});

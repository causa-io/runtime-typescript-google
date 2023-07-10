import { Injectable, Module } from '@nestjs/common';
import { Test } from '@nestjs/testing';
import { CollectionReference } from 'firebase-admin/firestore';
import { FirebaseModule } from '../index.js';
import { FirestoreCollection } from './collection.decorator.js';
import { FirestoreCollectionsModule } from './collections.module.js';
import { InjectFirestoreCollection } from './inject-collection.decorator.js';

@FirestoreCollection({ name: 'myCol', path: (doc) => doc.id })
class MyDocument {
  constructor(readonly id: string = '1234') {}
}

@Injectable()
class TestService {
  constructor(
    @InjectFirestoreCollection(MyDocument)
    readonly myCol: CollectionReference<MyDocument>,
  ) {}
}

@Module({
  imports: [
    FirebaseModule.forTesting(),
    FirestoreCollectionsModule.forRoot([MyDocument]),
  ],
  providers: [TestService],
})
class MyModule {}

describe('FirestoreCollectionsModule', () => {
  it('should inject a Firestore collection with a data converter', async () => {
    const testModule = await Test.createTestingModule({
      imports: [MyModule],
    }).compile();
    const testService = testModule.get(TestService);
    const document = new MyDocument('❄️');

    const actualCollection = testService.myCol;
    await actualCollection.doc('someDoc').set(document);
    const actualDocument = (await actualCollection.doc('someDoc').get()).data();

    expect(actualDocument).toBeInstanceOf(MyDocument);
    expect(actualDocument).toEqual({ id: '❄️' });
    expect(actualCollection).toBeInstanceOf(CollectionReference);
    expect(actualCollection.path).toEqual('myCol');
  });
});

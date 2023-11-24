export {
  FirestoreCollection,
  getReferenceForFirestoreDocument,
} from './collection.decorator.js';
export { FirestoreCollectionsModule } from './collections.module.js';
export {
  convertFirestoreTimestampsToDates,
  makeFirestoreDataConverter,
} from './converter.js';
export { wrapFirestoreOperation } from './error-converter.js';
export * from './errors.js';
export { InjectFirestoreCollection } from './inject-collection.decorator.js';

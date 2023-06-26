import { deleteApp } from 'firebase-admin/app';
import 'jest-extended';
import { getDefaultFirebaseApp } from './app.js';

describe('getDefaultFirebaseApp', () => {
  afterEach(async () => {
    await deleteApp(getDefaultFirebaseApp());
  });

  it('should return the default Firebase App and initialize it only once', () => {
    const actualApp1 = getDefaultFirebaseApp();
    const actualApp2 = getDefaultFirebaseApp();

    expect(actualApp1).toBe(actualApp2);
    expect(actualApp1.name).toBeString();
  });
});

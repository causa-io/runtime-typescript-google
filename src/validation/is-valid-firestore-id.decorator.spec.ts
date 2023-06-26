import { validate } from 'class-validator';
import 'jest-extended';
import { IsValidFirestoreId } from './is-valid-firestore-id.decorator.js';

describe('IsValidFirestoreId', () => {
  class MyDocument {
    public constructor(id: string) {
      this.id = id;
    }

    @IsValidFirestoreId()
    readonly id: string;
  }

  it('should return an error when the id is not a string', async () => {
    const errors = await validate(new MyDocument(123 as any));

    expect(errors).toEqual([
      expect.objectContaining({
        constraints: {
          isValidFirestoreId: `'id' cannot be '.', '..', or contain forward slashes.`,
        },
        property: 'id',
      }),
    ]);
  });

  it('should return an error when the id is `.`', async () => {
    const errors = await validate(new MyDocument('.'));

    expect(errors).toEqual([
      expect.objectContaining({
        constraints: {
          isValidFirestoreId: `'id' cannot be '.', '..', or contain forward slashes.`,
        },
        property: 'id',
      }),
    ]);
  });

  it('should return an error when the id is `..`', async () => {
    const errors = await validate(new MyDocument('..'));

    expect(errors).toEqual([
      expect.objectContaining({
        constraints: {
          isValidFirestoreId: `'id' cannot be '.', '..', or contain forward slashes.`,
        },
        property: 'id',
      }),
    ]);
  });

  it('should return an error when the id contains forward slashes', async () => {
    const errors = await validate(new MyDocument('oopsie/nope'));

    expect(errors).toEqual([
      expect.objectContaining({
        constraints: {
          isValidFirestoreId: `'id' cannot be '.', '..', or contain forward slashes.`,
        },
        property: 'id',
      }),
    ]);
  });

  it('should validate a correct Firestore ID', async () => {
    const errors = await validate(new MyDocument('yes+anything_else!✅'));

    expect(errors).toBeEmpty();
  });
});

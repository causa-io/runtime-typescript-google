import { PreciseDate } from '@google-cloud/precise-date';
import 'jest-extended';
import 'reflect-metadata';
import {
  SpannerColumn,
  getSpannerColumns,
  getSpannerColumnsMetadata,
} from './column.decorator.js';

type SomeJsonType = {
  a: number;
  b: string;
};

describe('SpannerColumn', () => {
  class NestedType {
    @SpannerColumn()
    otherColumn!: string;
  }

  class Test {
    @SpannerColumn()
    defaultName!: string;

    @SpannerColumn({ name: 'providedName' })
    overriddenName!: string;

    @SpannerColumn({ nestedType: NestedType })
    nestedColumn!: NestedType;

    @SpannerColumn({ nestedType: NestedType, nullifyNested: true })
    nullableNestedColumn!: NestedType | null;

    @SpannerColumn({ isBigInt: true })
    bigIntColumn!: bigint;

    @SpannerColumn()
    regularNumberColumn!: number;

    @SpannerColumn({ isInt: true })
    smallIntNumberColumn!: number;

    @SpannerColumn({ softDelete: true })
    regularDateColumn!: Date;

    @SpannerColumn({ isPreciseDate: true })
    preciseDateColumn!: PreciseDate;

    @SpannerColumn({ isJson: true })
    jsonColumn!: SomeJsonType;

    @SpannerColumn({ isJson: true })
    jsonArrayColumn!: SomeJsonType[];

    notAColumn!: number;
  }

  class Child extends Test {
    @SpannerColumn()
    anotherOne!: string;
  }

  describe('getSpannerColumnsMetadata', () => {
    it('should set the correct metadata for each column', () => {
      const test = new Test();
      const actualMetadata = getSpannerColumnsMetadata(
        test.constructor as { new (): Test },
      );

      expect(actualMetadata).toMatchObject({
        defaultName: { name: 'defaultName' },
        overriddenName: { name: 'providedName' },
        nestedColumn: {
          nestedType: NestedType,
          nullifyNested: false,
          isJson: false,
        },
        nullableNestedColumn: { nestedType: NestedType, nullifyNested: true },
        bigIntColumn: { isBigInt: true },
        regularNumberColumn: { isBigInt: false, isInt: false },
        smallIntNumberColumn: { isInt: true },
        preciseDateColumn: { isPreciseDate: true },
        regularDateColumn: { isPreciseDate: false, softDelete: true },
        jsonColumn: { isJson: true },
        jsonArrayColumn: { isJson: true },
      });
      expect(actualMetadata).not.toHaveProperty('notAColumn');
    });

    it('should inherit parents fields', async () => {
      const actualChildMetadata = getSpannerColumnsMetadata(Child);
      const actualParentMetadata = getSpannerColumnsMetadata(Test);

      expect(actualChildMetadata).toMatchObject({
        defaultName: { name: 'defaultName' },
        anotherOne: { name: 'anotherOne' },
      });
      expect(actualParentMetadata).not.toHaveProperty('anotherOne');
    });
  });

  describe('getSpannerColumns', () => {
    it('should return the list of columns', () => {
      const actualParentColumns = getSpannerColumns(Test);
      const actualChildColumns = getSpannerColumns(Child);

      expect(actualParentColumns).toIncludeSameMembers([
        'defaultName',
        'providedName',
        'nestedColumn_otherColumn',
        'nullableNestedColumn_otherColumn',
        'bigIntColumn',
        'regularNumberColumn',
        'smallIntNumberColumn',
        'preciseDateColumn',
        'regularDateColumn',
        'jsonColumn',
        'jsonArrayColumn',
      ]);
      expect(actualChildColumns).toIncludeSameMembers([
        'defaultName',
        'providedName',
        'nestedColumn_otherColumn',
        'nullableNestedColumn_otherColumn',
        'bigIntColumn',
        'regularNumberColumn',
        'smallIntNumberColumn',
        'preciseDateColumn',
        'regularDateColumn',
        'jsonColumn',
        'jsonArrayColumn',
        'anotherOne',
      ]);
    });
  });
});

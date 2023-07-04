import { PreciseDate } from '@google-cloud/precise-date';
import { Float, Int } from '@google-cloud/spanner';
import { SpannerColumn } from './column.decorator.js';
import {
  instanceToSpannerObject,
  spannerObjectToInstance,
} from './conversion.js';

const UNSAFE_INT = BigInt(Number.MAX_SAFE_INTEGER) + 10n;
const UNSAFE_INT_STR = UNSAFE_INT.toString();

describe('conversion', () => {
  class ChildEntity {
    constructor(data: Partial<ChildEntity> = {}) {
      Object.assign(this, data);
    }

    @SpannerColumn()
    defaultName!: string;

    @SpannerColumn({ name: 'otherName' })
    someName?: number;
  }

  class ParentEntity {
    constructor(data: Partial<ParentEntity> = {}) {
      Object.assign(this, data);
    }

    @SpannerColumn({ name: 'nestedColumn', nestedType: ChildEntity })
    childEntity!: ChildEntity | null;

    @SpannerColumn({
      name: 'nullableNestedColumn',
      nestedType: ChildEntity,
      nullifyNested: true,
    })
    nullableChildEntity!: ChildEntity | null;

    @SpannerColumn()
    otherProperty!: boolean;
  }

  class GrandParentEntity {
    constructor(data: Partial<GrandParentEntity> = {}) {
      Object.assign(this, data);
    }

    @SpannerColumn()
    highLevelProperty!: string;

    @SpannerColumn({ nestedType: ParentEntity })
    parentEntity!: ParentEntity | null;
  }

  class WrappedNumberEntity {
    constructor(data: Partial<WrappedNumberEntity> = {}) {
      Object.assign(this, data);
    }

    @SpannerColumn()
    someFloat!: number | null;

    @SpannerColumn({ isBigInt: true })
    someBigInt!: bigint | null;

    @SpannerColumn({ isInt: true })
    someSmallInt!: number | null;
  }

  class DateEntity {
    @SpannerColumn({ isPreciseDate: true })
    somePreciseDate!: PreciseDate;

    @SpannerColumn()
    someOtherDate!: Date;
  }

  type JsonType = { a: number; b: string };

  class JsonEntity {
    @SpannerColumn({ isJson: true })
    someJsonColumn!: JsonType;

    @SpannerColumn({ isJson: true })
    someJsonArrayColumn!: JsonType[];
  }

  class ArrayEntity {
    constructor(data: Partial<ArrayEntity> = {}) {
      Object.assign(this, data);
    }

    @SpannerColumn()
    stringArray!: string[];

    @SpannerColumn({ isInt: true })
    integerArray!: number[];

    @SpannerColumn()
    floatArray!: number[];

    @SpannerColumn({ isJson: true })
    jsonArray!: object[];
  }

  describe('spannerObjectToInstance', () => {
    it('should convert a flat plain object back to an instance', () => {
      const spannerObject = { defaultName: 'value', otherName: 5 };

      const actualInstance = spannerObjectToInstance(
        spannerObject,
        ChildEntity,
      );

      expect(actualInstance).toBeInstanceOf(ChildEntity);
      expect(actualInstance).toEqual({ defaultName: 'value', someName: 5 });
    });

    it('should convert a flat plain object to a nested instance', () => {
      const spannerObject = {
        nestedColumn_defaultName: 'value',
        nestedColumn_otherName: 5,
        nullableNestedColumn_defaultName: null,
        nullableNestedColumn_otherName: null,
        otherProperty: true,
      };

      const actualInstance = spannerObjectToInstance(
        spannerObject,
        ParentEntity,
      );

      expect(actualInstance).toBeInstanceOf(ParentEntity);
      expect(actualInstance.childEntity).toBeInstanceOf(ChildEntity);
      expect(actualInstance).toEqual({
        childEntity: { defaultName: 'value', someName: 5 },
        nullableChildEntity: null,
        otherProperty: true,
      });
    });

    it('should not nullify a nested entity by default', () => {
      const spannerObject = {
        nestedColumn_defaultName: null,
        nestedColumn_otherName: null,
        nullableNestedColumn_defaultName: null,
        nullableNestedColumn_otherName: null,
        otherProperty: true,
      };

      const actualInstance = spannerObjectToInstance(
        spannerObject,
        ParentEntity,
      );

      expect(actualInstance).toBeInstanceOf(ParentEntity);
      expect(actualInstance.childEntity).toBeInstanceOf(ChildEntity);
      expect(actualInstance).toEqual({
        childEntity: { defaultName: null, someName: null },
        nullableChildEntity: null,
        otherProperty: true,
      });
    });

    it('should handle big int numbers', () => {
      const spannerObject = {
        someFloat: new Float(1.5),
        someBigInt: new Int(UNSAFE_INT_STR),
        someSmallInt: new Int('42'),
      };

      const actualInstance = spannerObjectToInstance(
        spannerObject,
        WrappedNumberEntity,
      );

      expect(actualInstance).toBeInstanceOf(WrappedNumberEntity);
      expect(actualInstance).toEqual({
        someFloat: 1.5,
        someBigInt: UNSAFE_INT,
        someSmallInt: 42,
      });
    });

    it('should throw when unwrapping an unsafe number', () => {
      const spannerObject = {
        someFloat: new Float(1.5),
        someBigInt: new Int(UNSAFE_INT_STR),
        someSmallInt: new Int(UNSAFE_INT_STR),
      };

      expect(() => {
        spannerObjectToInstance(spannerObject, WrappedNumberEntity);
      }).toThrow('out of bounds');
    });

    it('should convert a PreciseDate to Date by default', () => {
      const spannerObject = {
        somePreciseDate: new PreciseDate(2021, 1, 1, 0, 0, 0, 0, 1, 1),
        someOtherDate: new PreciseDate(2021, 1, 1, 0, 0, 0, 0, 1, 1),
      };

      const actualInstance = spannerObjectToInstance(spannerObject, DateEntity);

      expect(actualInstance).toBeInstanceOf(DateEntity);
      expect(actualInstance).toEqual({
        somePreciseDate: spannerObject.somePreciseDate,
        someOtherDate: new Date(2021, 1, 1),
      });
      expect(actualInstance.somePreciseDate).toBeInstanceOf(PreciseDate);
      expect(actualInstance.someOtherDate).not.toBeInstanceOf(PreciseDate);
    });

    it('should forward JSON columns', () => {
      const spannerObject = {
        someJsonColumn: { a: 12, b: '🧠' },
        someJsonArrayColumn: [
          { a: 12, b: '🧠' },
          { a: 13, b: '🐶' },
        ],
      };

      const actualInstance = spannerObjectToInstance(spannerObject, JsonEntity);

      expect(actualInstance).toBeInstanceOf(JsonEntity);
      expect(actualInstance).toEqual({
        someJsonColumn: { a: 12, b: '🧠' },
        someJsonArrayColumn: [
          { a: 12, b: '🧠' },
          { a: 13, b: '🐶' },
        ],
      });
    });

    it('should convert arrays to JavaScript values', () => {
      const spannerObject = {
        stringArray: ['a', 'b'],
        integerArray: [new Int('1'), new Int('15')],
        floatArray: [new Float(1.0), new Float(2.0)],
        jsonArray: [{ obj: 1 }, { obj: 'yay' }],
      };

      const actualInstance = spannerObjectToInstance(
        spannerObject,
        ArrayEntity,
      );

      expect(actualInstance).toBeInstanceOf(ArrayEntity);
      expect(actualInstance).toEqual({
        stringArray: ['a', 'b'],
        integerArray: [1, 15],
        floatArray: [1.0, 2.0],
        jsonArray: [{ obj: 1 }, { obj: 'yay' }],
      });
    });
  });

  describe('instanceToSpannerObject', () => {
    it('should convert a flat entity to a plain object', () => {
      const instance = new ChildEntity({ defaultName: 'value', someName: 5 });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        ChildEntity,
      );

      expect(actualSpannerObject).toEqual({
        defaultName: 'value',
        otherName: new Float(5),
      });
    });

    it('should convert a nested entity to a flat plain object', () => {
      const childEntity = new ChildEntity({
        defaultName: 'value',
        someName: 5,
      });
      const instance = new ParentEntity({ childEntity, otherProperty: true });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        ParentEntity,
      );

      expect(actualSpannerObject).toEqual({
        nestedColumn_defaultName: 'value',
        nestedColumn_otherName: new Float(5),
        otherProperty: true,
      });
    });

    it('should set to null all columns for a nested object', () => {
      const instance = new ParentEntity({
        otherProperty: true,
        childEntity: null,
      });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        ParentEntity,
      );

      expect(actualSpannerObject).toEqual({
        nestedColumn_defaultName: null,
        nestedColumn_otherName: null,
        otherProperty: true,
      });
    });

    it('should set a single defined column in a nested object', () => {
      const instance = new ParentEntity({
        otherProperty: true,
        childEntity: { defaultName: 'test' },
      });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        ParentEntity,
      );

      expect(actualSpannerObject).toEqual({
        nestedColumn_defaultName: 'test',
        otherProperty: true,
      });
    });

    it('should handle a two-level hierarchy', () => {
      const instance = new GrandParentEntity({
        highLevelProperty: 'someValue',
        parentEntity: null,
      });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        GrandParentEntity,
      );

      expect(actualSpannerObject).toEqual({
        highLevelProperty: 'someValue',
        parentEntity_otherProperty: null,
        parentEntity_nestedColumn_otherName: null,
        parentEntity_nestedColumn_defaultName: null,
        parentEntity_nullableNestedColumn_otherName: null,
        parentEntity_nullableNestedColumn_defaultName: null,
      });
    });

    it('should handle bigint', () => {
      const instance = new WrappedNumberEntity({ someBigInt: UNSAFE_INT });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        WrappedNumberEntity,
      );

      expect(actualSpannerObject).toEqual({
        someBigInt: new Int(UNSAFE_INT_STR),
      });
    });

    it('should handle null values for big integers', () => {
      const instance = new WrappedNumberEntity({ someBigInt: null });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        WrappedNumberEntity,
      );

      expect(actualSpannerObject).toEqual({ someBigInt: null });
    });

    it('should wrap a small integer', () => {
      const instance = new WrappedNumberEntity({ someSmallInt: 46 });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        WrappedNumberEntity,
      );

      expect(actualSpannerObject).toEqual({ someSmallInt: new Int('46') });
    });

    it('should handle null values for small integers', () => {
      const instance = new WrappedNumberEntity({ someSmallInt: null });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        WrappedNumberEntity,
      );

      expect(actualSpannerObject).toEqual({ someSmallInt: null });
    });

    it('should handle null values for floats', () => {
      const instance = new WrappedNumberEntity({ someFloat: null });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        WrappedNumberEntity,
      );

      expect(actualSpannerObject).toEqual({ someFloat: null });
    });

    it('should convert arrays to Spanner objects when needed', () => {
      const instance = new ArrayEntity({
        stringArray: ['a', 'b', 'c'],
        integerArray: [3, 7, 9],
        floatArray: [3, 7, 9],
        jsonArray: [{ a: 1 }, { b: 2 }],
      });

      const actualSpannerObject = instanceToSpannerObject(
        instance,
        ArrayEntity,
      );

      expect(actualSpannerObject).toEqual({
        stringArray: ['a', 'b', 'c'],
        integerArray: [new Int('3'), new Int('7'), new Int('9')],
        floatArray: [new Float(3), new Float(7), new Float(9)],
        jsonArray: '[{"a":1},{"b":2}]',
      });
    });
  });
});
import 'reflect-metadata';
import {
  SpannerTable,
  getSpannerTableMetadataFromType,
} from './table.decorator.js';

describe('SpannerTable', () => {
  @SpannerTable({ primaryKey: ['id'] })
  class DefaultName {
    id!: string;
  }

  @SpannerTable({ primaryKey: ['id'], name: 'providedName' })
  class OverriddenName {
    id!: string;
  }

  class NotATable {}

  it('should use class name as default table name', () => {
    const obj = new DefaultName();
    const metadata = getSpannerTableMetadataFromType(
      obj.constructor as { new (): DefaultName },
    );

    expect(metadata).toEqual({ primaryKey: ['id'], name: 'DefaultName' });
  });

  it('should use provided name as table name', () => {
    const metadata = getSpannerTableMetadataFromType(OverriddenName);

    expect(metadata).toEqual({ primaryKey: ['id'], name: 'providedName' });
  });

  it('should return null if the class is not decorated', () => {
    const metadata = getSpannerTableMetadataFromType(NotATable);

    expect(metadata).toBeNull();
  });
});

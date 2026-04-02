import type {
  Fixture,
  NestJsModuleOverrider,
} from '@causa/runtime/nestjs/testing';
import { jest } from '@jest/globals';
import { SpannerOutboxSender } from './sender.js';

/**
 * A {@link Fixture} that mocks {@link SpannerOutboxSender.updateOutbox} and {@link SpannerOutboxSender.fetchEvents} to
 * no-ops. This avoids useless outbox updates and polling queries during tests, which can cause transaction conflicts,
 * especially with the Spanner emulator that only supports a single transaction at a time.
 */
export class SpannerOutboxFixture implements Fixture {
  /**
   * The spy on {@link SpannerOutboxSender.prototype.updateOutbox}, mocked to resolve immediately.
   */
  private updateOutboxSpy:
    | jest.Spied<SpannerOutboxSender['updateOutbox']>
    | undefined;

  /**
   * The spy on {@link SpannerOutboxSender.prototype.fetchEvents}, mocked to return an empty array.
   * This makes polling harmless without needing to set `SPANNER_OUTBOX_POLLING_INTERVAL` to `0`.
   */
  private fetchEventsSpy:
    | jest.Spied<SpannerOutboxSender['fetchEvents']>
    | undefined;

  async init(): Promise<NestJsModuleOverrider | undefined> {
    this.updateOutboxSpy = jest
      .spyOn(SpannerOutboxSender.prototype as any, 'updateOutbox')
      .mockResolvedValue(undefined);

    this.fetchEventsSpy = jest
      .spyOn(SpannerOutboxSender.prototype as any, 'fetchEvents')
      .mockResolvedValue([]);

    return;
  }

  async clear(): Promise<void> {
    this.updateOutboxSpy?.mockClear();
    this.fetchEventsSpy?.mockClear();
  }

  async delete(): Promise<void> {
    this.updateOutboxSpy?.mockRestore();
    this.updateOutboxSpy = undefined;
    this.fetchEventsSpy?.mockRestore();
    this.fetchEventsSpy = undefined;
  }
}

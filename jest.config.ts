import type { Config } from 'jest';

const config: Config = {
  clearMocks: true,
  coverageDirectory: '../coverage',
  collectCoverageFrom: [
    '**/*.{js,ts}',
    '!**/index.ts',
    '!**/*.spec.ts',
    '!**/*.test.{ts,js}',
  ],
  rootDir: 'src',
  testEnvironment: 'node',
  setupFilesAfterEnv: ['jest-extended/all'],
  testMatch: ['**/*.spec.ts'],
  extensionsToTreatAsEsm: ['.ts'],
  moduleFileExtensions: ['js', 'ts'],
  transform: {
    '^.+\\.ts$': ['ts-jest', { useESM: true }],
  },
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};

export default config;

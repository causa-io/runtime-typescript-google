/** @type {import('jest').Config} */
const config = {
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
  setupFiles: ['dotenv/config'],
  setupFilesAfterEnv: ['jest-extended/all'],
  testMatch: ['**/*.spec.ts'],
  extensionsToTreatAsEsm: ['.ts'],
  moduleFileExtensions: ['js', 'ts'],
  transform: {
    '^.+\\.(t|j)s?$': ['@swc/jest', { sourceMaps: 'inline' }],
  },
  moduleNameMapper: {
    '^(\\.{1,2}/.*)\\.js$': '$1',
  },
};

export default config;

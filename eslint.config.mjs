import prettier from 'eslint-plugin-prettier/recommended';
import { defineConfig } from 'eslint/config';
import tseslint from 'typescript-eslint';

export default defineConfig({
  extends: [...tseslint.configs.recommended, prettier],
  rules: {
    '@typescript-eslint/no-explicit-any': 'off',
  },
});

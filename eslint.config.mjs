import globals from 'globals'
import pluginJs from '@eslint/js'
import tseslint from 'typescript-eslint'

/** @type {import('eslint').Linter.Config[]} */
export default [
  {
    files: ['src/**/*.{js,mjs,cjs,ts}'],
  },
  { languageOptions: { globals: globals.browser } },
  pluginJs.configs.recommended,
  ...tseslint.configs.recommended,
  {
    ignores: ['src/generated/', '**/lib/', 'coverage/', '__tests__/'],
  },
  {
    languageOptions: {
      globals: { process: 'readable' },
    },
  },
]

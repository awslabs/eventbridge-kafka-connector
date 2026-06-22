module.exports = {
    extends: ['@commitlint/config-conventional'],
    // Any rules defined here will override rules from @commitlint/config-conventional
    rules: {
        // warn only
        'subject-case': [1, 'always', 'lower-case'],
        // warn only (long bodies, e.g. from Dependabot changelogs, should not fail CI)
        'body-max-length': [1, 'always', 200],
        // warn only (Dependabot changelog/URL lines routinely exceed 100 chars)
        'body-max-line-length': [1, 'always', 100]
    },
}
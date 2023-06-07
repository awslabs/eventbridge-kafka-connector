module.exports = {
    extends: ['@commitlint/config-conventional'],
    // Any rules defined here will override rules from @commitlint/config-conventional
    rules: {
        'subject-case': [1, 'always', 'lower-case'],
    },
}
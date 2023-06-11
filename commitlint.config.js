module.exports = {
    extends: ['@commitlint/config-conventional'],
    // Any rules defined here will override rules from @commitlint/config-conventional
    rules: {
        // warn only
        'subject-case': [1, 'always', 'lower-case'],
        // fail
        'body-max-length': [2, 'always', 200]
    },
}
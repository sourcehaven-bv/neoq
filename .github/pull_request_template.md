<!-- managed by sourcehaven/repo-templates -->
<!-- markdownlint-disable-file MD041 -->
## What and why

<!-- What does this change do, and why? Link the issue. -->

## How it was tested

<!-- Describe how you verified the change. -->

## Security checklist

- [ ] No secrets or credentials in code, configuration, tests or commit history.
- [ ] Tests and fixtures use no production data.
- [ ] New or updated dependencies are needed, maintained and free of known
      vulnerabilities.
- [ ] New functionality or significant changes: the issue describes the security
      requirements and threats.

Where relevant (OWASP):

- [ ] Input from users or other systems is validated on the server (type,
      length, format, allowlist).
- [ ] Queries and commands use parameters, not string concatenation (SQL
      injection, command injection).
- [ ] Output is encoded for its context (HTML, URL, JSON) to prevent cross-site
      scripting.
- [ ] Every new endpoint or action checks authentication and authorisation on
      the server.
- [ ] File paths and URLs from input are restricted (path traversal, server-side
      request forgery).
- [ ] Error messages and logs contain no secrets, tokens or personal data;
      security events are logged.
- [ ] Sensitive data uses standard cryptography and encrypted connections; no
      custom crypto.

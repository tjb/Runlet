# Security Policy

Runlet is pre-release. Security fixes will be handled as quickly as practical,
but there is no formal support window yet.

## Reporting a Vulnerability

Please do not open a public GitHub issue for a suspected vulnerability.

Report security issues by emailing:

```text
tjbobella@gmail.com
```

Include:

- affected module or version, if known
- a description of the issue
- reproduction steps or proof of concept, if available
- any known impact or mitigation

## Scope

Security reports are most useful for issues involving:

- unsafe file handling
- checkpoint corruption or data loss caused by malformed input
- dependency vulnerabilities in published modules
- denial-of-service behavior caused by untrusted input
- Spring Boot integration behavior that exposes sensitive state

Runlet does not currently provide authentication, authorization, network
services, or distributed execution.

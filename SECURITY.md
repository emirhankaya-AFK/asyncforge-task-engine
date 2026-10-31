# Security policy

This is a **local-only demo**. Bind API to loopback. Never expose it to the internet without TLS, network controls, rate limiting, audited authentication and secrets handling. Do not submit untrusted code, shell commands, arbitrary imports, URLs or integration credentials. There are only four hardcoded demo task types; each has typed, bounded data. Worker subprocess isolation is **not** an operating-system security sandbox and must not run untrusted arbitrary code.

No current production projects, databases, credentials or resources are necessary. `.env` must remain ignored. PostgreSQL must be a fresh isolated database; integration and benchmark scripts refuse other DB names before resetting tables. Report vulnerabilities through private communication with the owner, not public issues containing secrets.

Retries provide at-least-once execution. A fenced DB write cannot guarantee exactly-once external side effects. Production application integrations are explicitly out of scope.

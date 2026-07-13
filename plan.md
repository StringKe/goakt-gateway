# Project quality plan

## Current baseline

- [x] I1-I4: library correctness, concurrency fencing, CI, API compatibility, and documentation.
- [x] I5: local OrbStack lifecycle evidence for three replicas, PDB, HPA metrics, restart, and rollback.
- [x] Quality gates: build, vet, race, lint, Redis, Valkey, actionlint, and govulncheck.
- [x] Deployment integration: `Server.Shutdown`, `WithDrainOnShutdown`, health checks, resource limits, and rollback evidence.

The repository documentation is the operational source of truth. Follow `README.md` and
`CONTRIBUTING.md` for executable commands and deployment contracts.

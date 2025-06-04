# AGENTS Instructions

Codex must follow these instructions when working with this repository.

- If you modify any Rust code (anything outside of Markdown or other docs), run `just lint` followed by `just test` before committing.
  - `just lint` formats the workspace and runs clippy.
  - `just test` runs the test suite and will automatically connect to a running PostgreSQL instance or start one via `scripts/postgres.sh`.
- For documentation-only changes, running `just lint` is sufficient.


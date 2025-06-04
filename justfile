# List available recipes
default:
    @just db

# Start PostgreSQL database using Docker
db:
    @bash scripts/postgres.sh
    # Migrations are applied by the repository at runtime

# Setup PostgreSQL without Docker for Codex
ai-setup:
    @bash scripts/postgres_codex.sh
    # Migrations are applied by the repository at runtime

# Build the project
build:
    cargo build

# Build with optimizations
build-release:
    cargo build --release

# Run all tests
test: db
    cargo test --workspace --all-features

# Run clippy lints
lint:
    cargo fmt --all
    cargo clippy --workspace --all-features -- -D warnings

# Run example
example example="bank":
    cd examples/{{example}} && cargo run

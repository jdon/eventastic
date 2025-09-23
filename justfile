# List available recipes
default:
    @just db

# Start PostgreSQL database using Docker
db:
    @bash scripts/postgres.sh
    sqlx migrate run --source eventastic_postgres/migrations/

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

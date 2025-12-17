Data Load Engine – Project Debrief & System Architecture
Recipe-Driven Data Loading Framework (Python + Optional Rust Core)
1. Project Summary

This project defines a data loading engine built around declarative recipes, enabling robust EL workflows with minimal code. Recipes describe:

The source (Postgres, S3, API, etc.)

Any transformations (rename, cast, enrich, audit)

The destination (Redshift, Snowflake, S3, DuckDB, etc.)

Incremental logic with cursor/watermark recovery

Runtime behavior such as batching, retries, parallelism

The library delivers:

A clean Python developer experience

A declarative configuration style similar to Chef cookbooks

Stateful incremental loads

A pluggable connector system

Optional Rust acceleration for performance-critical operations

The end goal: define what you want to sync, not how, and the engine handles reliability, batching, retries, and state.

2. High-Level Goals

Declarative recipe-based data pipelines

Idempotent & incremental loads

Reusable base recipes via extends:

Transform pipelines (Python/Rust hybrid)

Clean DB-agnostic connector abstraction

CLI + Python API

Optional Arrow/Polars Rust core

3. Example Recipe
name: load_customers_from_postgres_to_redshift

extends: base_recipe.yaml

source:
  type: postgres
  conn_id: src_postgres
  table: public.customers
  incremental:
    strategy: cursor
    cursor_column: updated_at

transform:
  steps:
    - type: rename_columns
      mapping:
        fname: first_name
        lname: last_name

destination:
  type: redshift
  conn_id: dw_redshift
  table: dw.customers
  write_mode: merge
  merge_keys: [id]

runtime:
  batch_size: 5000   # override base
  parallelism: 4

4. System Architecture
        ┌────────────────┐
        │  Recipe (YAML) │
        └───────▲────────┘
                │ Parsing & Validation
                ▼
     ┌────────────────────────┐
     │   Recipe Model Layer   │
     │ (Pydantic-based schema)│
     └───────────┬────────────┘
                 │ Build Execution Plan
                 ▼
     ┌────────────────────────┐
     │     Execution Engine   │
     ├─────────┬──────────────┤
     │ State   │ Transform     │
     │ Mgmt    │ Pipeline      │
     └─────┬───┴───────┬──────┘
           │           │
           ▼           ▼
   ┌─────────────┐  ┌──────────────┐
   │ Source Plug- │  │ Destination   │
   │     ins      │  │    Plug-ins   │
   └──────┬───────┘  └───────┬───────┘
          │                  │
          ▼                  ▼
     Data Stream ---> Transform ---> Write

5. Core Components
5.1 Recipe Model Layer

Responsible for:

Parsing YAML/JSON

Schema validation via Pydantic

Applying inheritance via extends:

Merging default values

Constructing execution plans

Objects:

Recipe

SourceConfig

TransformConfig

DestinationConfig

RuntimeConfig

5.2 Connectors – Plug-in Architecture

Two key interfaces:

class Source(Protocol):
    def read_batches(self, state: State) -> Iterable[Batch]: ...

class Destination(Protocol):
    def write_batch(self, batch: Batch, state: State): ...


Connectors implemented as plug-ins registered by type.

Examples:

Sources: Postgres, MySQL, S3, REST, BigQuery

Destinations: Redshift, Snowflake, S3, DuckDB

5.3 Transform Pipeline

Supports:

rename columns

cast columns

add/drop columns

audit fields

custom Python functions

optional Rust transform kernels

Acts similarly to a mini-dbt layer inside the runtime engine.

5.4 Incremental State Management

State backends:

Local JSON

S3

DynamoDB

SQL table backend

The incremental state is used for:

Cursor values

High-watermarks

Sync checkpoints

Recovery after failure

Backend protocol:

class StateBackend(Protocol):
    def load(self, recipe_name: str) -> dict: ...
    def save(self, recipe_name: str, state: dict): ...

5.5 Execution Engine

Handles:

Batch retrieval

Transform pipeline execution

Destination writes

Incremental state updates

Retry logic

Logging/metrics

Core loop:

for batch in source.read_batches(state):
    batch = transformer.apply(batch)
    destination.write_batch(batch, state)
    state_backend.save(recipe.name, state)

6. Rust Engine (Optional)

Rust is introduced for:

Arrow-formatted batching

Polars-based transforms

CSV/Parquet read/write at high throughput

IO parallel orchestration without Python GIL

Bindings via PyO3.

7. Developer Experience
7.1 Python API
from datacook import Recipe, run_recipe
from datacook.state import LocalStateBackend

recipe = Recipe.from_yaml("recipes/customers.yaml")
run_recipe(recipe, LocalStateBackend(".state"))

7.2 CLI
datacook init
datacook run recipe.yaml
datacook validate recipe.yaml
datacook show-state <recipe>
datacook list-connectors

8. Roadmap
v0.1 – Prototype

Python-only engine

Minimal connectors (Postgres, CSV, DuckDB, S3)

Local JSON state

Basic transforms

v0.2 – Reliable MVP

Parallelism

Retry logic

Better logging/metrics

S3/DynamoDB state

CLI

v0.3 – Rust Engine

Arrow Batches

Rust transform DSL

Rust I/O readers/writers

9. Differentiators vs Existing Tools
Feature	This Engine	dlt-hub	Airbyte	Singer
Declarative recipes	Yes	Limited	No	No
Inheritance (extends:)	Yes	No	No	No
Python-first	Yes	Yes	No	No
Rust acceleration	Optional	No	No	No
Composable macros	Yes	No	No	No
Embeddable	Yes	Partial	No	No
10. Repository Structure (Suggested)
datacook/
  core/
    engine.py
    state.py
    batch.py
  models/
    recipe.py
    source_config.py
    destination_config.py
  connectors/
    postgres/
    redshift/
    s3/
    duckdb/
  transforms/
    rename.py
    cast.py
    pipeline.py
  cli/
    main.py
  rust/
    Cargo.toml
    src/
      lib.rs
examples/
  recipes/
tests/
docs/
README.md

11. Recipe Inheritance via extends: — Deep Dive

extends: is a central feature that allows recipes to be composed, layered, and reused, similar to configuration inheritance in Chef cookbooks or Terraform modules.

11.1 How It Works
extends: base_recipe.yaml


Inheritance applies as:

Load parent recipe (base_recipe.yaml)

Deep merge parent → child

Child overrides any field

Child can also delete inherited fields

Result is fully resolved before execution

11.2 Benefits

Shared defaults for all pipelines

Environment layering (base → dev → prod)

DRY: eliminate duplicate configuration

Standardized audit and governance transforms

Simplified domain-specific pipeline templates

11.3 Merge Rules
Scalars → override
parent.runtime.batch_size = 20000  
child.runtime.batch_size = 5000  
→ effective: 5000

Dicts → deep merge

Unspecified keys are inherited.

Lists → behavior depends:

Transform steps → concatenated

Macro lists → overridden

Connector streams → name-based merge (future)

Delete semantics
delete:
  - transform.steps
  - destination.merge_keys

11.4 Example Base Recipe
# base_recipe.yaml
default_runtime:
  batch_size: 20000
  max_retries: 5
  parallelism: 2

transform:
  steps:
    - type: add_column
      name: _loaded_at
      value: now()
    - type: add_column
      name: _source
      value: "{{ recipe.name }}"

11.5 Example Child Recipe
extends: base_recipe.yaml

name: load_orders

source:
  type: postgres
  conn_id: src_db
  table: orders

transform:
  steps:
    - type: rename_columns
      mapping:
        order_dt: order_date

runtime:
  batch_size: 5000

Effective transform steps:

add _loaded_at

add _source

rename order_dt → order_date

11.6 Inheritance Patterns
Pattern 1 – Multi-tiered recipes
base_recipe.yaml
↓
domain_defaults/customers.yaml
↓
recipes/load_customers_us.yaml

Pattern 2 – Environment layering
base.yaml
↓
env/dev.yaml
↓
env/prod.yaml

Pattern 3 – Reusable transform cookbooks
audit.yaml
pseudonymize.yaml
anonymize.yaml
clean_numeric.yaml

Pattern 4 – Connector templates
postgres_default.yaml
redshift_default.yaml
s3_default.yaml

11.7 Macros + Inheritance

Macros can also be inherited:

# base_recipe.yaml
macros:
  audit:
    - type: add_column
      name: _audit_ts
      value: now()

# child
extends: base_recipe.yaml
transform:
  use: [audit]

11.8 Multi-level Inheritance

Fully supported:

recipe_c.yaml -->
recipe_b.yaml -->
recipe_a.yaml


The engine resolves deepest parent first.

Cycle detection prevents infinite loops.

11.9 Future Extensions

Multiple inheritance
(extends: [a.yaml, b.yaml] with explicit merge rules)

Remote recipe registry

Namespaced inheritance (extends: datacook://defaults/postgres)

URL-based recipes for multi-team ecosystems

12. Conclusion

This architecture enables a powerful, extensible, and high-performance data loading system centered on recipes, state, and clean abstractions. The ability to layer recipes (extends:) gives the system a unique advantage: reproducible, standardized, maintainable pipelines that work across teams and environments.

# config

Non-secret project configuration.

This folder contains schemas, examples, and static policy/configuration files that are safe to commit.

Typical contents:
- JSON schemas
- non-secret execution settings
- contract definitions
- example environment files
- policy and threshold documents
- requirement split notes in `config/requirements/`

Do **not** commit real secrets here.
Use `config/alpaca.env.example` as the template and keep the real `config/alpaca.env` local only.

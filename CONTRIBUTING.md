# Contributing to Hoard

Thanks for your interest in contributing.

## Process
1. Fork the repo and create a feature branch.
2. Make changes with tests where appropriate.
3. Open a pull request.
4. Wait for review by the maintainer.

## Expectations
- Keep changes small and focused.
- Do not edit `docs/` (not tracked).
- Preserve CLI and MCP tool schema compatibility.
- Add tests for new behavior.

## Code Style
- Prefer clear, explicit code over clever shortcuts.
- Keep functions small and readable.

## Security
Connectors run as trusted code in v1. Avoid adding anything that reads arbitrary files or makes network requests without explicit opt‑in.

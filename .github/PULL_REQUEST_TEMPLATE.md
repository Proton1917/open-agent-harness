## What changed

Describe the observable behavior, the invariant it preserves, and why the change belongs in the harness core.

## Evidence

List the tests, reproducible inputs/outputs, or source-level argument that support every behavioral claim. Do not paste or upload private comparison material.

## Authorship and review

- [ ] I have read and can explain every submitted line, including AI-assisted code.
- [ ] I identified every AI-assisted part below and documented how I independently verified it.
- [ ] I independently obtained any lawful comparison material needed for parity claims.
- [ ] This is an original implementation; no private binary, extracted source, prompt, asset, credential, or personal data is included.
- [ ] The harness core and primary executable remain Rust; ancillary tooling is transparent, scoped, and justified.
- [ ] The patch contains no placeholder, dead branch, unexplained suppression, or speculative compatibility claim.

### AI-assistance disclosure

List the files or sections that used AI assistance, what the model produced, and the tests or source-level review you performed yourself. Write `None` only when no AI system contributed to the patch.

## Verification

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo test --locked --all-targets`
- [ ] `cargo clippy --locked --all-targets -- -D warnings`
- [ ] `cargo build --locked --release`
- [ ] `scripts/audit-harness.sh`
- [ ] Complete test and release build logs contain zero warnings.
- [ ] Success, failure, resource-limit, permission, and privacy paths are covered where relevant.
- [ ] `README.md` and `MIGRATION.md` make no claim beyond the implementation.

By opening this pull request, I confirm that I followed the repository’s `CONTRIBUTING.md`.

# Contributing to NetbaIoT

## Contributor licensing

The proposed Contributor License Agreement is available in
[English](CLA.md) and [中文](CLA.zh-CN.md). Both versions are draft 1.1 and contain
the same numbered terms. The draft lets contributors retain copyright while
expressly granting the Maintainer independent commercial and proprietary
sublicensing rights, alongside continued availability of an open source version
under `AGPL-3.0-or-later`.

The CLA is a draft awaiting finalization; a signing requirement and automated
signing workflow are not yet active. Publishing the draft, submitting a pull
request or adding a commit sign-off does not constitute signing this agreement.
The project's current open source license remains [AGPL-3.0-or-later](LICENSE).
Individuals and organizations may use, modify, deploy and redistribute the open
source version, including for commercial purposes, under the AGPL without signing
a CLA or purchasing a commercial license. All existing Rust crates retain their
AGPL license declarations; the CLA does not split or replace those licenses.

### Contributions and pull request process

Once a finalized CLA is formally adopted, the signing requirement is intended to
cover external source code, patches, tests, documentation and other original
material intentionally submitted for inclusion in NetbaIoT under Section 2.
Ordinary issue reports, discussions and use of the software alone are outside
that requirement. An existing signature may cover later submissions within its
recorded scope and signing capacity; a new signature is not required for every
pull request. A different rights holder or changed signing capacity requires a
new or supplemental authorization record.

Until formal adoption, contributors may use the existing pull request process;
the draft does not impose a new signature or merge gate. For a contribution:

1. Identify the problem, scope of the change and whether you submit personally or
   for an organization. Keep the submission traceable to its authors and rights
   holders through pull request and commit references.
2. Identify third-party material, its source, version, affected files, license,
   required notices and known restrictions. Distinguish your original additions
   from underlying work or code written by other contributors. Follow
   [SECURITY.md](SECURITY.md) for undisclosed vulnerabilities and do not publish
   confidential material or private authorization documents in an issue or PR.
3. Run the checks applicable to the change and state their results in the PR.
4. After formal adoption, use the published signing channel if a valid record
   covering your contribution is missing. The Maintainer must verify the signing
   record, rights scope and any organization authority before accepting material
   as covered by the CLA. A PR or `Signed-off-by` line alone is not that record.

An individual signs for rights they own or are authorized to grant. A company or
other organization signs through a representative who can bind it, with the
authorized submitters and contribution scope recorded. Where an employer or
client owns relevant rights, an employee's personal signature does not replace
the required organization authorization or agreement. Do not infer authorization
from a company email address or GitHub organization membership alone.

Historical contributions require a supplemental document expressly agreed by both
parties that identifies the covered submissions. Keep their records linked to
the signer and rights holder. Neither adopting a CLA nor updating its version
automatically covers earlier contributions.

Third-party code requires a separate rights review. AGPL compatibility does not
by itself permit closed source commercial relicensing. Review the actual license
or separately obtained permission before including the material in a commercial
license; preserve applicable obligations, replace it or exclude it where the
required rights are unavailable. The CLA cannot grant rights the signer lacks.

### Decisions and legal review before adoption

Before adopting the agreement, the Maintainer must complete the legal identity
behind `sskycn`, contact information, governing law, dispute resolution and
controlling language. No jurisdiction or language is selected by default.

The Maintainer must also decide the adoption date, contribution scope, published
signing channel, identity and organization authority verification process, record
custodian, privacy notice, access controls and lawful retention policy. Confirm
how historical contributions and any version transition will be handled before
announcing a signing requirement.

Qualified counsel for the chosen jurisdictions must review the final text,
including the irrevocable grants and commercial sublicensing chain, removal
exceptions and continuing AGPL obligations, direct downstream patent grants,
patent litigation termination and the treatment of defensive counterclaims,
project succession and any required consent or formalities, electronic signature
evidence, moral rights, privacy, governing law and dispute resolution. The draft
does not constitute a finding that these arrangements are legally enforceable.
It is not ready for formal signing until those decisions and reviews are complete.

### Signing records and future automation

The signing process must obtain explicit acceptance of the finalized terms,
including Sections 3 and 4. Follow Section 10 for the required record: legal
identity and linked GitHub account, signing capacity and authority, exact version
and immutable copies of both texts, signed-text revision or digest, timestamp with
time zone, consent evidence, contribution scope, agreed controlling language,
historical attachments where needed and acknowledgment of receipt.

Store completed forms, contact details, signatures and organization authority
documents privately with restricted access. Do not commit them to this public
repository or expose them in PR comments, Actions logs or artifacts. Collect only
necessary information; government identification numbers or identity document
copies are not routine requirements. Explain data purposes, access and retention
before collection and keep a copy of the applicable privacy notice with the record.

After adoption, a GitHub App or GitHub Actions check could compare PR contributors
and rights holders against a private signing register for the relevant agreement
version and scope, reporting only the necessary status. Start with a non-blocking
trial and manual review of organizational contributions, historical attachments
and ambiguous authorship before making any check required. Review permissions,
data handling, private storage and record export before selecting a service.
[CLA assistant](https://github.com/cla-assistant/cla-assistant) is an example of a
tool that supports PR signing and version-specific signing status; account-based
confirmation alone does not replace the legal identity, authority and consent
records required here. No such automation or required CLA check is enabled by
this change.

A DCO or Git `Signed-off-by` may serve a different provenance process, but does
not automatically grant the independent commercial sublicensing rights in this
CLA. An automated check must verify express acceptance of the actual finalized
CLA rather than treating a commit sign-off as a substitute.

## Setup

Use Rust 1.88 or newer, Cargo, and Python 3.9 or newer. The release checks run on
Rust 1.88.0 and stable. MQTT interoperability checks also need `mosquitto`,
`mosquitto_pub`, `mosquitto_sub`, and OpenSSL. Mosquitto is a test/reference broker,
not a NetbaIoT runtime dependency.

```bash
git clone https://github.com/sskycn/netbaiot.git
cd netbaiot
cargo build --locked
cargo install --locked cargo-audit
```

Try `cargo run --locked -p netbaiot-cli -- demo --once` first. The
[Quick Start](docs/quick-start.md) also retains the manual webhook examples.

## Unified maintenance entry

Use `cargo xtask check` for daily changes, `cargo xtask check mqtt` for the complete
MQTT/SDK gates, and `cargo xtask check release` for release preparation. Add
`--audit` to daily check when cargo-audit is installed. Regenerate configuration
artifacts with `cargo xtask schema` / `cargo xtask config-reference`; CI checks drift.
See [maintenance](docs/maintenance.md) for required tools, CI parts and explicit
BLOCKED behavior. The bottom-level commands below remain available.

## Before opening a PR

```bash
cargo fmt --all -- --check
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo test --locked --workspace --all-features
cargo audit
```

Keep the lockfile and all workspace package versions consistent. Describe the
problem, resulting behavior, compatibility impact, and exactly which checks you
ran. Mark checks you could not run explicitly; do not present configured resource
limits or old benchmark results as measured current capacity.

## MQTT changes

Changes to parsers, the broker, sessions, QoS, recovery, or MQTT 5 require focused
raw state-machine tests and the protocol/interoperability gates:

```bash
cargo build --locked -p netbaiot-server
python3 tests/mqtt_protocol_regressions.py --repo . --output target/mqtt-audit/pr.json
python3 -m unittest discover -s tests/mqtt_conformance -p 'test_*.py' -v
python3 tests/mqtt_conformance/run.py --release-gate
python3 tests/mqtt_conformance/v5_smoke.py
python3 tests/mqtt_conformance/v5_mosquitto.py
cargo build --locked -p netbaiot-device-sdk --example device_mqtt
python3 tests/run_device_profile_mosquitto.py
python3 tests/measure_device_profile.py
```

Run relevant fuzz targets for decoder changes. The **MQTT Device Profile decoder
fuzz smoke** Actions workflow supports manual dispatch; local runs use nightly and
`cargo-fuzz`. A NetbaIoT normative failure must fail the release gate, even when a
reference broker observation is timing-sensitive.

## Security and reliability changes

Add targeted tests for the affected boundary: malformed inputs, count/byte
rollback, cancellation, lifecycle races, invalidation, required sink ACKs, recovery,
or live-session command ownership. Run relevant subprocess restart, outage,
slow-sink, and fuzz checks. Report measurements and untested failure modes honestly.
Follow [SECURITY.md](SECURITY.md) for undisclosed vulnerabilities.

## Scope and architecture

Prefer focused PRs with one logical change. Do not silently change public protocol
semantics; include compatibility review and serialization tests when they change.
The runtime remains database-free, memory-first, and bounded. Business systems own
durable business data and offline commands.

Read [AGENTS.md](AGENTS.md) for the full architecture and correctness constraints.
Use [docs/release-template.md](docs/release-template.md) when preparing a release.

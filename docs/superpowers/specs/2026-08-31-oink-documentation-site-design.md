# OINK Documentation Site Design

**Date:** 2026-08-31  
**Status:** Approved for implementation planning  
**Repository:** `mapseekai/eventbus-contract`

## Context

`eventbus-contract` is a six-crate Rust workspace that provides object-safe event-bus contracts, an in-process backend, a Redis Streams backend, and future-facing outbox and integration-event contracts. The repository currently explains the project through `README.md`, examples, migration notes, changelog entries, rustdoc, and tests, but it does not provide a navigable project documentation site.

OINK is a local-first Hugo theme that publishes native Markdown as searchable HTML, printable sections, Markdown representations, and agent-facing indexes. A temporary spike using OINK v1.0.0 proved that the current project content can be rendered with offline search, Mermaid diagrams, Markdown output, and `llms.txt`, and that a warning-strict production build succeeds.

## Goals

- Publish a Chinese-first documentation site for Rust developers evaluating or adopting `eventbus-contract`.
- Keep contributor and maintainer material available without obscuring the user onboarding path.
- Keep exact API signatures authoritative on docs.rs instead of duplicating rustdoc.
- Build and deploy reproducibly through GitHub Actions and GitHub Pages.
- Preserve Markdown, print, RSS, offline search, Mermaid, and `llms.txt` outputs.
- Make document claims traceable to current public APIs, runnable examples, tests, migration notes, and changelog entries.

## Non-goals

- Generating prose automatically from Rust source during every build.
- Replacing docs.rs or publishing a second copy of the complete API reference.
- Adding a blog, book, comments, analytics, or external search service.
- Publishing English or French translations in the initial release.
- Changing the Rust workspace API, crate behavior, or release process.
- Committing generated `public/` output.

## Audience and Desired Reader Outcomes

The primary audience is Rust developers who want to decide whether the crate fits their system and then integrate it correctly. After reading the site, they should be able to:

1. identify which crate and feature they need;
2. run the memory-backed quickstart without an external service;
3. choose ACK, retry, dead-letter, ordering, and backpressure behavior without invalid combinations;
4. move from the memory backend to Redis Streams;
5. find exact API signatures on docs.rs;
6. understand which outbox and integration components are contracts awaiting a reference implementation.

Contributors and maintainers are a secondary audience. Their material lives in a separate contributor section covering workspace boundaries, backend implementation, testing, and release operations.

## Selected Repository Integration

The documentation source will live in a standalone `website/` directory inside the existing repository.

This design was chosen over two alternatives:

- Root-level Hugo files would minimize path depth but mix Go/Hugo files and content directories into the Rust workspace root.
- A separate documentation repository would isolate deployment but make source and documentation versions easier to desynchronize.

`website/` keeps the site versioned with the code while isolating its toolchain. It also avoids the existing root `.gitignore` rule for `docs/`, which is used for internal planning artifacts.

## Repository Layout

```text
eventbus-contract/
├── .github/workflows/
│   └── docs.yml
├── website/
│   ├── README.md
│   ├── .gitignore
│   ├── go.mod
│   ├── go.sum
│   ├── hugo.yaml
│   ├── assets/
│   │   └── icons/logo.svg
│   ├── static/
│   │   └── favicon.svg
│   ├── data/home/
│   │   └── zh.yaml
│   └── content/
│       ├── _index.md
│       └── docs/
│           ├── _index.md
│           ├── start/
│           ├── guides/
│           ├── concepts/
│           ├── reference/
│           └── contributing/
└── README.md
```

The site starts from the small OINK Starter baseline, but the Starter Blog, Book, English, and French samples are not retained.

## Information Architecture

```text
Home
└── Documentation
    ├── Start
    │   ├── Project overview
    │   ├── Installation and features
    │   └── Five-minute quickstart
    ├── Guides
    │   ├── Memory backend
    │   ├── Redis Streams
    │   ├── Watermill interoperability
    │   └── Competing consumers
    ├── Concepts
    │   ├── Message envelope
    │   ├── Publisher, Subscriber, and Handler
    │   ├── ACK, retry, and dead-letter handling
    │   ├── Delivery guarantees and idempotency
    │   └── Ordering and backpressure
    ├── Reference
    │   ├── Crate and feature matrix
    │   ├── Configuration constraints
    │   ├── Error model
    │   └── Examples and commands
    └── Contributing
        ├── Workspace architecture
        ├── Implementing a backend
        ├── Testing strategy
        └── Release process
```

The home page links directly to the quickstart, architecture, delivery semantics, GitHub repository, and docs.rs. It does not advertise optional surfaces that the project does not maintain.

## Content Ownership and Data Flow

Documentation facts use this authority order:

1. current public Rust API;
2. runnable examples under `crates/eventbus-contract/examples/`;
3. behavior verified by tests;
4. `README.md`, `MIGRATION-0.2.md`, and `CHANGELOG.md`.

Tutorial snippets should be adapted from runnable examples rather than invented independently. Exact type signatures and exhaustive member lists link to docs.rs. Migration and release pages summarize their purpose and link back to the repository-owned source files or GitHub Releases.

OINK reads Markdown and project data from `website/`, combines them with the pinned theme module, and produces `website/public/`. GitHub Actions uploads that static directory to the Pages deployment API. No runtime service is required.

## OINK and Hugo Configuration

- OINK is pinned to `github.com/pgsty/oink v1.0.0` in `website/go.mod`.
- `website/go.mod` declares Go 1.27.
- CI installs Hugo Extended 0.165.0.
- The only enabled language is Simplified Chinese, and unsuffixed `.md` files are the source documents.
- The configured production URL is `https://mapseekai.github.io/eventbus-contract/`.
- Deployment overrides `baseURL` with the value produced by `actions/configure-pages`.
- `github_repo` is `https://github.com/mapseekai/eventbus-contract`.
- `github_branch` is `main`.
- `github_subdir` is `website`, so edit and history links target the correct source paths.
- Offline search, dark mode, Mermaid, foldable navigation, and backlinks are enabled.
- Home output includes HTML, RSS, Markdown, and LLMS.
- Page output includes HTML and Markdown.
- Section output includes HTML, RSS, print, and Markdown.
- `enableGitInfo` remains enabled.

`website/.gitignore` will ignore generated site state relative to the site root: `public/`, `resources/`, `.hugo_build.lock`, and the local Hugo cache. Documentation source remains tracked. Keeping these rules inside `website/` avoids mixing the documentation-site change with unrelated root ignore rules.

## Local Development

`website/README.md` documents the required tools and two supported commands:

```bash
cd website
hugo server
```

```bash
cd website
hugo --cleanDestinationDir --gc --minify --environment production \
  --printPathWarnings --panicOnWarning
```

Local setup requires Git, Go 1.27 or newer, and Hugo Extended 0.165.0. Contributors may use a temporary or package-managed Hugo installation; the repository does not install system tools.

## GitHub Actions and Pages Deployment

`.github/workflows/docs.yml` is independent from the existing Rust CI and crate publishing workflows.

It supports pull requests, pushes to `main`, and manual dispatch. The build job:

1. checks out full history with `fetch-depth: 0`;
2. configures Go from `website/go.mod`;
3. disables local Go and Hugo module workspaces;
4. installs Hugo Extended 0.165.0;
5. downloads the OINK version pinned in `website/go.mod`;
6. performs a warning-strict production build from `website/`;
7. validates required generated artifacts and path-sensitive links.

Pull requests stop after validation. A push to `main` uploads `website/public/` and deploys it through the GitHub Pages deployment API. The workflow uses no `gh-pages` branch.

Repository Settings must set Pages build source to GitHub Actions. This one-time setting is outside repository code and must be performed by a repository administrator.

## Validation Strategy

### Build-time validation

The production build uses:

```bash
hugo --cleanDestinationDir --gc --minify --environment production \
  --printPathWarnings --panicOnWarning
```

The workflow then verifies:

- the home page and quickstart page exist;
- `sitemap.xml`, `robots.txt`, the 404 page, Markdown outputs, and `llms.txt` exist;
- an offline search index exists;
- the architecture page contains Mermaid diagram data and loads the local Mermaid runtime;
- canonical and asset links retain the `/eventbus-contract/` project path;
- edit and history URLs include `website/content/`.

The validation must be deterministic and must not depend on the deployed site already existing.

### Content validation

Content is checked against current source, examples, and tests while it is authored. Reader testing uses fresh agents without the implementation conversation to answer realistic questions from the generated documentation. At minimum, the test questions cover:

- installing the right feature;
- completing the memory-backed quickstart;
- distinguishing ACK, NACK, and retry;
- understanding exactly-once constraints and application-level idempotency;
- identifying unpublished 0.3-facing crates;
- finding contributor guidance for a new backend.

Any incorrect, ambiguous, or context-dependent answer results in a targeted documentation correction followed by a new strict build.

## Failure Handling and Rollback

- Any Hugo warning or validation failure blocks artifact upload and deployment.
- The deploy job depends on the successful build job.
- A documentation failure does not alter Rust crate CI behavior.
- `website/public/` is never committed or edited manually.
- Production rollback republishes the last known-good Git commit or reverts the offending documentation commit.
- OINK or Hugo upgrades occur in separate changes from content edits, keeping toolchain rollback attributable.

## Files Expected to Change

- Add the `website/` source tree and pinned Hugo Module files.
- Add `.github/workflows/docs.yml`.
- Add `website/.gitignore` for site build outputs.
- Update root `README.md` with the published documentation entry point.
- Add implementation planning artifacts under `docs/superpowers/`.

No Rust source or public API symbol is expected to change.

## Acceptance Criteria

The integration is complete when:

1. the site has the approved Chinese information architecture and no Starter sample surfaces;
2. a local warning-strict production build succeeds;
3. expected HTML, Markdown, search, Mermaid, print, RSS, sitemap, robots, 404, and LLMS outputs are present;
4. generated project-subpath and GitHub source links are correct;
5. the GitHub Actions workflow validates PRs and is ready to deploy `main` through Pages;
6. root and website README files document discovery and local operation;
7. independent reader testing finds no material ambiguity in the required onboarding and delivery-semantics questions;
8. the Rust workspace and existing CI behavior remain unchanged.

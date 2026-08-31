# eventbus-contract documentation site

This directory contains the Chinese OINK documentation site published at
<https://mapseekai.github.io/eventbus-contract/>.

## Requirements

- Git
- Go 1.27 or newer
- Hugo Extended 0.165.0

Confirm that `hugo version` contains `extended`.

## Preview

```bash
cd website
hugo server
```

Open <http://localhost:1313/>.

## Production build

```bash
cd website
hugo --cleanDestinationDir --gc --minify --environment production \
  --printPathWarnings --panicOnWarning
bash scripts/verify-site.sh
```

## Theme upgrades

Keep OINK and Hugo upgrades in a dedicated change. Update OINK in `go.mod`,
refresh `go.sum` with `go mod download github.com/pgsty/oink`, run the strict
build, and inspect the home page, quickstart, diagrams, search, Markdown output,
and `llms.txt` before merging.

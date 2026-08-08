#!/usr/bin/env bash
#
# Build and publish declaro-persistum, then prove it is installable.
#
# `uv publish` exits 0 whether or not an upload lands. It prints "Uploading"
# either way. Its exit code therefore carries no information, and a release
# can appear to succeed while nothing reached the index. This script does not
# trust it: it verifies against PyPI and fails loudly if the version is not
# there and installable.
#
# The index also lags the upload by a short time, so a single check straight
# after publishing can report a false negative. The script polls.
#
# Usage:
#   PYPI_TOKEN=... scripts/publish.sh            # publish the version in pyproject.toml
#   PYPI_TOKEN=... scripts/publish.sh --dry-run  # build and check, do not upload
#
set -euo pipefail

PKG="declaro-persistum"
DIST_NAME="declaro_persistum"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$HERE"

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

VERSION="$(grep -m1 '^version = ' pyproject.toml | cut -d'"' -f2)"
[[ -n "$VERSION" ]] || { echo "FAIL: no version in pyproject.toml" >&2; exit 1; }
echo "==> version $VERSION"

# Refuse to publish a version that already exists. PyPI rejects a re-upload,
# but uv reports that the same way it reports success, so catch it here.
if curl -fsS "https://pypi.org/pypi/${PKG}/json" 2>/dev/null \
   | python3 -c "import json,sys; sys.exit(0 if '$VERSION' in json.load(sys.stdin)['releases'] else 1)"; then
    echo "FAIL: $VERSION is already on PyPI. Bump the version." >&2
    exit 1
fi

echo "==> tests"
uv run pytest tests/ -q --ignore=tests/stress

echo "==> build"
rm -rf dist/
uv build

WHEEL="dist/${DIST_NAME}-${VERSION}-py3-none-any.whl"
SDIST="dist/${DIST_NAME}-${VERSION}.tar.gz"
[[ -f "$WHEEL" && -f "$SDIST" ]] || { echo "FAIL: expected $WHEEL and $SDIST" >&2; exit 1; }

if [[ "$DRY_RUN" == "1" ]]; then
    echo "==> dry run: built but not uploaded"
    exit 0
fi

: "${PYPI_TOKEN:?set PYPI_TOKEN}"

echo "==> upload"
# Exit code deliberately ignored: it is 0 whether or not anything landed.
uv publish --token "$PYPI_TOKEN" "$WHEEL" "$SDIST" 2>&1 \
    | sed -E "s/pypi-[A-Za-z0-9_-]+/<redacted>/g" || true

echo "==> verifying against the index (the upload command cannot be trusted)"
SCRATCH="$(mktemp -d)"
trap 'rm -rf "$SCRATCH"' EXIT

for attempt in 1 2 3 4 5 6 7 8; do
    rm -rf "$SCRATCH/v"
    uv venv "$SCRATCH/v" -q >/dev/null 2>&1
    if uv pip install --python "$SCRATCH/v/bin/python" --no-cache -q \
           "${PKG}==${VERSION}" >/dev/null 2>&1; then
        INSTALLED="$("$SCRATCH/v/bin/python" -c \
            "from importlib.metadata import version; print(version('$PKG'))")"
        if [[ "$INSTALLED" == "$VERSION" ]]; then
            echo "==> PUBLISHED and installable: ${PKG} ${VERSION}"
            exit 0
        fi
        echo "FAIL: index served $INSTALLED, expected $VERSION" >&2
        exit 1
    fi
    echo "    attempt $attempt: not installable yet, waiting for the index"
    # Cache-busted request; also paces the retries.
    curl -s "https://pypi.org/simple/${PKG}/?cb=$RANDOM" \
         -H "Cache-Control: no-cache" >/dev/null || true
done

echo "FAIL: ${PKG} ${VERSION} is not installable from PyPI after 8 attempts." >&2
echo "      The upload command may have reported success without landing." >&2
exit 1

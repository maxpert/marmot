#!/bin/bash
# Download and setup mimalloc v3.0.11 sources for CGO integration

set -e

MIMALLOC_VERSION="v3.0.11"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
TARGET_DIR="$PROJECT_ROOT/pkg/mimalloc/c"
TEMP_DIR=$(mktemp -d)

cleanup() {
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

echo "Downloading mimalloc ${MIMALLOC_VERSION}..."
curl -sL "https://github.com/microsoft/mimalloc/archive/refs/tags/${MIMALLOC_VERSION}.tar.gz" | tar -xz -C "$TEMP_DIR"

EXTRACTED_DIR="$TEMP_DIR/mimalloc-${MIMALLOC_VERSION#v}"

# Remove existing mimalloc sources if present
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

echo "Copying source files..."
cp -r "$EXTRACTED_DIR/include" "$TARGET_DIR/"
cp -r "$EXTRACTED_DIR/src" "$TARGET_DIR/"

# Copy license
cp "$EXTRACTED_DIR/LICENSE" "$TARGET_DIR/"

echo "mimalloc ${MIMALLOC_VERSION} installed to $TARGET_DIR"
echo "Done."

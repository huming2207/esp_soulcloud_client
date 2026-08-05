#!/bin/sh
# Host build & run for the soulcloud protocol tests (no ESP-IDF required).
# Usage: tests/host/build.sh
set -e

DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$DIR/../.." && pwd)"
MPACK="$ROOT/external/mpack/src/mpack"
OUT="$DIR/out"

CFLAGS="-I$ROOT/src -I$MPACK -DMPACK_HAS_CONFIG=1 -Wall -Wextra -O2 -g"
CXXFLAGS="$CFLAGS -std=c++17"

mkdir -p "$OUT"

gcc $CFLAGS -c "$MPACK/mpack-common.c" -o "$OUT/mpack-common.o"
gcc $CFLAGS -c "$MPACK/mpack-expect.c" -o "$OUT/mpack-expect.o"
gcc $CFLAGS -c "$MPACK/mpack-platform.c" -o "$OUT/mpack-platform.o"
gcc $CFLAGS -c "$MPACK/mpack-reader.c" -o "$OUT/mpack-reader.o"
gcc $CFLAGS -c "$MPACK/mpack-writer.c" -o "$OUT/mpack-writer.o"

g++ $CXXFLAGS -c "$ROOT/src/protocol.cpp" -o "$OUT/protocol.o"
g++ $CXXFLAGS -c "$DIR/protocol_test.cpp" -o "$OUT/protocol_test.o"

g++ -o "$OUT/protocol_test" \
    "$OUT/protocol_test.o" "$OUT/protocol.o" \
    "$OUT/mpack-common.o" "$OUT/mpack-expect.o" \
    "$OUT/mpack-platform.o" "$OUT/mpack-reader.o" \
    "$OUT/mpack-writer.o"

# fixtures are generated from the real soulcloud.js codecs; copy them next
# to the test binary (see gen_fixtures.ts in the repo docs)
if [ -d "$DIR/fixtures" ]; then
    mkdir -p "$OUT/fixtures"
    cp "$DIR"/fixtures/*.msgpack "$OUT/fixtures/"
fi

"$OUT/protocol_test" "$OUT"

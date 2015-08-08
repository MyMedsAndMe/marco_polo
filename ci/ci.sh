#!/bin/bash

set -e

ROOT=$(dirname $(cd "$(dirname "$0")"; pwd))
CI_DIR="$ROOT/ci"
BUILD_DIR="$ROOT/tmp"

ODB_DOWNLOAD_URL="http://orientdb.com/download.php?email=unknown@unknown.com&file=orientdb-community-${ORIENTDB_VERSION}.tar.gz&os=linux"
echo $BUILD_DIR

mkdir -v -p "$BUILD_DIR"
cd "$BUILD_DIR"

echo "==> Downloading OrientDB version $ORIENTDB_VERSION"
wget -q -O "$ORIENTDB_VERSION.tar.gz" "$ODB_DOWNLOAD_URL"

echo "==> Extracting OrientDB"
tar -xzf "$ORIENTDB_VERSION.tar.gz"

echo "==> Removing archive"
rm -v "$ORIENTDB_VERSION.tar.gz"

ODB_DIR="$BUILD_DIR/orientdb-community-${ORIENTDB_VERSION}"

cd "$ODB_DIR"

echo "==> Setting up OrientDB"
chmod -R +x ./bin

if [[ -e "$CI_DIR/configs/orientdb-server-config_${ORIENTDB_VERSION}.xml" ]]; then
    cp -v "$CI_DIR/configs/orientdb-server-config_${ORIENTDB_VERSION}.xml" ./config/orientdb-server-config.xml
else
    cp -v "$CI_DIR/configs/orientdb-server-config.xml" ./config/
fi

cp -v "$CI_DIR/configs/orientdb-server-log.properties" ./config/


echo "==> Creating an orientdb-console script"
SCRIPT_PATH="$BUILD_DIR/orientdb-console"
echo "#!/bin/bash" >> "$SCRIPT_PATH"
echo "$ODB_DIR/bin/console.sh \"\$@\"" >> "$SCRIPT_PATH"
chmod +x "$SCRIPT_PATH"

./bin/server.sh </dev/null &>/dev/null &

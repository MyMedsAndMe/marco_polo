#!/bin/bash

set -e

ROOT=$(dirname $(cd "$(dirname "$0")"; pwd))
CI_DIR="$ROOT/ci"
BUILD_DIR="$ROOT/tmp"


if [[ "${ORIENTDB_VERSION}" == *"SNAPSHOT"* ]]; then
    ODB_DOWNLOAD_URL="https://oss.sonatype.org/service/local/artifact/maven/content?r=snapshots&g=com.orientechnologies&a=orientdb-community&v=${ORIENTDB_VERSION}&e=tar.gz"
else
    ODB_DOWNLOAD_URL="http://central.maven.org/maven2/com/orientechnologies/orientdb-community/${ORIENTDB_VERSION}/orientdb-community-${ORIENTDB_VERSION}.tar.gz"
fi
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
cp -rv "$CI_DIR/configs/cert" ./config/


echo "==> Creating an orientdb-console script"
SCRIPT_PATH="$BUILD_DIR/orientdb-console"
echo "#!/bin/bash" >> "$SCRIPT_PATH"
echo "$ODB_DIR/bin/console.sh \"\$@\"" >> "$SCRIPT_PATH"
chmod +x "$SCRIPT_PATH"

./bin/server.sh </dev/null &>/dev/null &

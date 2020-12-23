#!/usr/bin/env bash

echo "Starting http server on http://localhost:3000/ and opening browser"
echo "Kill this process (CTRL+C) to stop serving the docs"
echo "This uses docsify. Install it if you don't aready have it"
(sleep 1; open http://localhost:3000/) &
SCRIPTS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
docsify serve $SCRIPTS_DIR/../docs

#!/bin/bash

if [ $# -ne 4 ]; then
  echo "Missing one or more arguments!"
  echo "Usage: $0 app branch src_pkg src_sha256"
  exit 2
fi

app=$1
branch=$2
src_pkg=$3
src_sha256=$4

cat <<EOF | gpg --no-tty --clearsign | base64 | curl -k -XPOST --data @- https://notary.spreedly.com/notarize
{
  "event": "deploy",
  "ttl_seconds": 500,
  "app": "$app",
  "branch": "$branch",
  "ansible_vars": {
    "src_pkg": "$src_pkg",
    "src_sha256": "$src_sha256"
  }
}
EOF

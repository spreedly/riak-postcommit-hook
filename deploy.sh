#!/bin/bash

cat <<EOF | gpg --no-tty --clearsign | base64 | curl -k -XPOST --data @- https://notary.spreedly.com/notarize
{
  "event": "deploy",
  "ttl_seconds": 500,
  "app": "riak-postcommit-hook",
  "branch": "deploy"
}
EOF

#!/bin/sh
# Writes deploy/bots.txt: accounts for the bots, with random tokens. The bots' port is not
# published outside the compose network, but the tokens should still not be guessable.
set -eu
out="$(dirname "$0")/bots.txt"
{
  echo "# The bots' accounts: id token max-open-orders messages-per-second."
  for id in 1 2 3 4 5 6 7 8 9 10 11 12; do
    token=$(od -An -N8 -tu8 /dev/urandom | tr -d ' ')
    echo "$id $token 1000 1000"
  done
} > "$out"
echo "wrote $out"

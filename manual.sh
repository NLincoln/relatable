#! /bin/bash
set -e;

filename=${1:-foobar}

rm -f $filename
cargo build
run() {
   ./target/debug/cli $1 $filename $2
}

run create
run run-file testing.sql

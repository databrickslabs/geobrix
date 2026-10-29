#!/bin/bash
# symbolize-check.sh ELF SYMBOL TARBALL: the debuginfo tarball symbolizes an exported address of a shipped (stripped,
# re-pathed) ELF file. Reads ELF's GNU build-id, extracts .build-id/<xx>/<rest>.debug from TARBALL, and runs addr2line
# on SYMBOL's address from ELF's dynamic symbol table: it must print SYMBOL and a file:line. Prints
# `ok: symbolized: ...` or `SYMBOLIZE FAILED: ...` (exit 1). Needs readelf, tar (xz) and addr2line (the build image).
set -uo pipefail
elf="$1"; sym="$2"; tarball="$3"
fail() { echo "SYMBOLIZE FAILED: $*"; exit 1; }
[ -f "$elf" ] && [ -f "$tarball" ] || fail "no $elf or no $tarball"
id="$(readelf -n "$elf" 2> /dev/null | awk '/Build ID:/ && !id {id = $3} END {print id}')"
[ -n "$id" ] || fail "$elf has no build-id"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT
member=".build-id/${id:0:2}/${id:2}.debug"
tar -xJf "$tarball" -C "$work" "$member" 2> /dev/null || tar -xJf "$tarball" -C "$work" "./$member" 2> /dev/null \
    || fail "$member ($elf) is not in $tarball"
addr="$(readelf --dyn-syms -W "$elf" | awk -v s="$sym" '$8 == s && !a {a = $2} END {print a}')"
[ -n "$addr" ] || fail "no $sym in the dynamic symbols of $elf"
got="$(addr2line -f -C -e "$work/$member" "0x$addr" | tr '\n' ' ')"
case "$got" in "$sym "*:[0-9]*) ;; *) fail "addr2line of $sym (0x$addr) in $(basename "$elf") gives '$got'" ;; esac
echo "ok: symbolized: $(basename "$elf") 0x$addr -> $got(debug file $member)"

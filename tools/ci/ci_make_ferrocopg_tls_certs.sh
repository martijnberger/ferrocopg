#!/bin/sh

set -eu

cert_dir=${1:?usage: ci_make_ferrocopg_tls_certs.sh CERT_DIR}
mkdir -p "$cert_dir"

openssl req -x509 -newkey rsa:2048 -nodes -sha256 -days 1 \
    -subj "/CN=ferrocopg test CA" \
    -keyout "$cert_dir/ca.key" \
    -out "$cert_dir/ca.crt"

openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=localhost" \
    -keyout "$cert_dir/server.key" \
    -out "$cert_dir/server.csr"

printf '%s\n' \
    'subjectAltName=DNS:localhost,IP:127.0.0.1' \
    'extendedKeyUsage=serverAuth' > "$cert_dir/server.ext"

openssl x509 -req -sha256 -days 1 \
    -in "$cert_dir/server.csr" \
    -CA "$cert_dir/ca.crt" \
    -CAkey "$cert_dir/ca.key" \
    -CAcreateserial \
    -extfile "$cert_dir/server.ext" \
    -out "$cert_dir/server.crt"

openssl req -new -newkey rsa:2048 -nodes -sha256 \
    -subj "/CN=certuser" \
    -keyout "$cert_dir/client.key" \
    -out "$cert_dir/client.csr"

printf '%s\n' 'extendedKeyUsage=clientAuth' > "$cert_dir/client.ext"

openssl x509 -req -sha256 -days 1 \
    -in "$cert_dir/client.csr" \
    -CA "$cert_dir/ca.crt" \
    -CAkey "$cert_dir/ca.key" \
    -CAcreateserial \
    -extfile "$cert_dir/client.ext" \
    -out "$cert_dir/client.crt"

chmod 600 "$cert_dir/server.key" "$cert_dir/client.key"

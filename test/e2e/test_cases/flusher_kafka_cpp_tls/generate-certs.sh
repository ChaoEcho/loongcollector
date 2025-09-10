#!/bin/bash

# Copyright 2025 iLogtail Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

CERT_DIR="/tmp/certs"
PASSWORD="changeit"

echo "Generating TLS certificates..."

mkdir -p $CERT_DIR

echo "Generating CA private key..."
openssl genrsa -out $CERT_DIR/ca.key 4096

echo "Generating CA certificate..."
openssl req -new -x509 -key $CERT_DIR/ca.key -sha256 -subj "/C=CN/ST=CA/L=CA/O=CA/OU=CA/CN=ca" -days 3650 -out $CERT_DIR/ca.crt

echo "Generating server private key..."
openssl genrsa -out $CERT_DIR/server.key 4096

echo "Generating server certificate request..."
openssl req -new -key $CERT_DIR/server.key -out $CERT_DIR/server.csr -subj "/C=CN/ST=CA/L=CA/O=Server/OU=Server/CN=kafka"
cat > $CERT_DIR/server.ext << EOF
[v3_req]
subjectAltName = @alt_names
basicConstraints = CA:FALSE
keyUsage = nonRepudiation, digitalSignature, keyEncipherment
[alt_names]
DNS.1 = kafka
DNS.2 = localhost
EOF

echo "Generating server certificate..."
openssl x509 -req -in $CERT_DIR/server.csr -CA $CERT_DIR/ca.crt -CAkey $CERT_DIR/ca.key -CAcreateserial -out $CERT_DIR/server.crt -days 365 -extensions v3_req -extfile $CERT_DIR/server.ext

echo "Generating PKCS12 keystore..."
openssl pkcs12 -export -in $CERT_DIR/server.crt -inkey $CERT_DIR/server.key -out $CERT_DIR/server.keystore.p12 -name kafka -password pass:$PASSWORD
echo "$PASSWORD" > $CERT_DIR/keystore_creds
echo "$PASSWORD" > $CERT_DIR/key_creds

chmod 644 $CERT_DIR/*

echo "Certificate generation completed!"
echo "Generated files:"
ls -la $CERT_DIR/

touch $CERT_DIR/.certs_ready

echo "Certificate generation service completed, keeping container running..."
tail -f /dev/null

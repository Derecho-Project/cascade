#!/bin/bash
# A script that generates RSA key pairs for the nodes in chain_pipeline_cfg
# The "server" nodes must all have the same private key, and the "client" node must have the public key for that private key

# OpenSSL incantations to generate a key pair for the service
openssl genpkey -algorithm rsa -outform PEM -out private_key.pem
openssl pkey -in private_key.pem -out service_public_key.pem -pubout -outform PEM

# Copy the private key to the server nodes
for folder in n{0..3}; do
    cp private_key.pem $folder
done

# Copy the public key to the external client node
cp service_public_key.pem n4

# Generate a private key for the client, even though it won't use it, to make Derecho happy
openssl genpkey -algorithm rsa -outform PEM -out n4/private_key.pem
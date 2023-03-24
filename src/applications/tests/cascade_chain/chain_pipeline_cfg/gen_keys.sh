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

# Copy the public key to the external client node and the backup nodes
for folder in n{4..7}; do
    cp service_public_key.pem $folder
done

# Generate a private key for the client, even though it won't use it, to make Derecho happy
openssl genpkey -algorithm rsa -outform PEM -out n4/private_key.pem

# Generate a private key for the backup site nodes
openssl genpkey -algorithm rsa -outform PEM -out backup_private_key.pem
for folder in n{5..7}; do
    cp backup_private_key.pem $folder/private_key.pem
done

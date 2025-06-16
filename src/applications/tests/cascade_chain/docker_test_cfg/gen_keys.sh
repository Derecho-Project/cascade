#!/bin/bash
# A script that generates RSA key pairs for the nodes in docker_test_cfg
# The "server" nodes must all have the same private key, and the "client" node must have
# the servers' public keys.

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
openssl genpkey -algorithm rsa -outform PEM -out n7/private_key.pem

# Generate a key pair for the backup site
openssl genpkey -algorithm rsa -outform PEM -out backup_private_key.pem
openssl pkey -in backup_private_key.pem -out backup_public_key.pem -pubout -outform PEM

# Copy the private key to the backup server nodes
for folder in n{4..6}; do
    cp backup_private_key.pem $folder/private_key.pem
done

# Copy the public key to the external client node
cp backup_public_key.pem n7

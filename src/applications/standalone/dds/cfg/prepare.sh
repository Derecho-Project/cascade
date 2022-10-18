#!/usr/bin/env bash

cascade_client create_object_pool /dds/metadata VCSS 0
sleep 3
cascade_client create_object_pool /dds/tiny_text VCSS 0
sleep 3
cascade_client create_object_pool /dds/big_chunk VCSS 0

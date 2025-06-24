# CascadeChain Test Configurations

This folder contains the specialized client for CascadeChain, chain_only_client, and three sets of configuration files that can be used to deploy CascadeChain for testing. The CMake target for this folder builds the client executable and copies the configuration folders to the output directory.

## Configuration 1: chain_pipeline_cfg

This is the standard testing configuration for CascadeChain. It contains per-node configuration folders for 8 nodes, which should configure them in the following layout:

* Primary site: n0-n3
    * PersistentCascadeStore: n0, n2
    * SignatureCascadeStore: n1, n3
    * CascadeMetadataService: n0
* Backup site: n5-n7
    * PersistentCascadeStore: n5
    * SignatureCascadeStore: n6, n7
    * CascadeMetadataService: n5
* Client: n4

At each site, the SignatureCascadeStore nodes should be configured as the replicas of a WanAgent site; in other words, WanAgent site 0 is n1 and n3, and WanAgent site 1 is n6 and n7. The Derecho contact node (initial leader) for the primary site is n0, and the Derecho contact node for the backup site is n5.

**Configuration files**: Achieving this layout requires each node to load several configuration files. Some of the configuration files are stored in the per-node configuration folders, and some are stored in the parent directory (chain_pipeline_cfg) and symlinked into the per-node folders during the CMake build process; the symlinked files are the same for every node at a site. After building with CMake, each node configuration folder in the output directory will contain the following files:

* derecho_node-fractus.cfg
* derecho_node-local.cfg
* wanagent-fractus.json
* wanagent-local.json
* derecho-fractus.cfg (*symlink to ../derecho-fractus.cfg or ../backup_derecho-fractus.cfg depending on node*)
* derecho-local.cfg (*symlink to ../derecho-local.cfg or ../backup_derecho-fractus.cfg depending on node*)
* dfgs.json (*symlink to ../dfgs.json or ../backup_dfgs.json depending on node*)
* layout.json (*symlink to ../layout.json*) **or** backup_layout.json (*symlink to ../backup_layout.json*), depending on node
* udl_dlls.cfg (*symlink to ../udl_dlls.cfg or ../backup_udl_dlls.cfg depending on node*)

Note that most of these files have two versions, one that ends in `-fractus` and one that ends in `-local`. The files with the `-fractus` suffix are intended to be used when the test layout is deployed to the Fractus cluster at Cornell, where each node can reside on its own server. The files with the `-local` suffix are intended to be used when testing the system locally on your own computer, where each node is simply a process running in a different terminal window.

The symlinked files link to the "primary" version of the file on n0-n4, and to the "backup" version of the file on n5-n7. Most of them use the same link name regardless of which file they are linking to because the desired final file name is the same on both sites. However, for layout.json, the backup site nodes use a link named backup_layout.json because the name of this file is not fixed by Derecho or Cascade; it is set in the derecho.cfg file, so the backup site's derecho.cfg specifies that the layout file is named backup_layout.json.

### One-Time Setup

**Choosing a test configuration**: Before running a test, you must choose which version of the configuration files to use and rename those files so they will be found by Cascade. If deploying the test locally, rename each file ending in `-local` to remove the `-local` suffix, so e.g. derecho_node-local.cfg becomes derecho_node.cfg. If deploying the test on Fractus, rename each file ending in `-fractus` to remove the suffix, so e.g. derecho_node-fractus.cfg becomes derecho_node.cfg. For the files that are symlinks, rename the symlink (in the per-node folder), not the file it points to. The final result should be that each per-node folder contains files with the following names:

* derecho_node.cfg
* wanagent.json
* derecho.cfg (*symlink to ../derecho-fractus.cfg, ../backup_derecho-fractus.cfg, ../derecho-local.cfg, or ../backup_derecho-fractus.cfg, depending on choice*)
* dfgs.json (*symlink to ../dfgs.json or ../backup_dfgs.json*)
* layout.json (*symlink to ../layout.json*) **or** backup_layout.json (*symlink to ../backup_layout.json*)

These exact file names are required by Derecho or Cascade, except for layout.json (or backup_layout.json), where the name of the file is specified by the key `json_layout_file` in derecho.cfg.

**Generating keys**: CascadeChain requires each site (primary and backup) to have an RSA key pair that is shared among all the replicas at that site. The backup site needs to know the primary site's public key, and the client must know both sites' public keys. To generate these keys, you can run `gen_keys.sh`, which will also copy the PEM files for each key to the correct folder. Specifically, each node folder will contain the following keys after running the script:

* n0-n3: private_key.pem (primary site's private key)
* n4: service_public_key.pem (primary site's public key), backup_public_key.pem (backup site's public key), private_key.pem (client node's private key, unused)
* n5-n7: service_public_key.pem (primary site's public key), private_key.pem (backup site's private key)

Note that `gen_keys.sh` generates a new set of key pairs every time it is run, so it should only be run on one machine. If you are deploying to Fractus, you will need to manually copy (via scp) the generated keys in each node folder to the other machines where those nodes will actually run.

### Running a Test

Each node folder contains a symlink to the script `run.sh`, which can be used to launch either a server or client process. First, start the primary site by running the script from within the n0-n3 folders, like this:

```
cascade_chain/chain_pipeline_cfg/n0$ ./run.sh server
```

Then start the backup site nodes the same way:

```
cascade_chain/chain_pipeline_cfg/n5$ ./run.sh server
```

When the server nodes have finished starting up they will print the message `Press Enter to Shutdown` (and possibly some other messages, depending on the log level you have configured in their derecho_local.cfg). You can then launch the client from within the n4 folder:

```
cascade_chain/chain_pipeline_cfg/n4$ ./run.sh client
cmd>
```

You can type "help" to see the list of client commands; note that the chain_only_client has a different set of commands than the standard Cascade client. Before running any test commands against CascadeChain, you will need to run two setup commands: `load_service_key` and `setup_object_pools`. First, the client needs to be told the name of the PEM file that contains the primary site's public key:

```
cmd> load_service_key service_public_key.pem
-> Succeeded.
```

Second, the CascadeChain servers need to be instructed to create the "/storage" and "/signatures" object pools that CascadeChain expects to exist:

```
cmd> setup_object_pools
node(0) replied with version:8589934593,previous_version=1747777887166791,previous_version_by_key:-1,ts_us:-1
create_object_pool is done.
node(0) replied with version:8589934595,previous_version=1747777887197272,previous_version_by_key:8589934593,ts_us:-1
create_object_pool is done.
-> Succeeded.
```

These two steps only need to be done once per test session; you can then run any number of "put" and "get" commands, such as `put_with_signature` and `get_and_verify`.

When you are ready to end the test, make sure to use the `quit` command on the client before shutting down the servers (by pressing Enter). If you shut down the servers first, the client will hang when it attempts to send a "graceful disconnect" to the server it was communicating with.

## Configuration 2: primary_test_cfg

This is the configuration used for performance testing of the CascadeChain primary site by itself, with no backup sites configured. It contains per-node configuration files for 8 nodes that should configure them in the following layout:

* CascadeMetadataService: n0
* PersistentCascadeStore: n1, n2, n3
* SignatureCascadeStore: n4, n5, n6
* Client: n7

The Derecho contact node is n0.

**Configuration files**: Achieving this layout requires each node to load several configuration files, although fewer than chain_pipeline_cfg since WanAgent is not used. Some of the configuration files are stored in the per-node configuration folders, and some are stored in the parent directory (primary_test_cfg) and symlinked into the per-node folders during the CMake build process. After building with CMake, each node configuration folder in the output directory will contain the following files:

* derecho_node-fractus.cfg
* derecho-fractus.cfg (*symlink to ../derecho-fractus.cfg*)
* dfgs.json (*symlink to ../dfgs.json*)
* layout.json (*symlink to ../layout.json*)
* test_private_key.pem
* udl_dlls.cfg (*symlink to ../udl_dlls.cfg*)

Following the same naming convention as chain_pipeline_cfg, the derecho config files have the suffix `-fractus` to indicate they should be used on the Fractus cluster, but there is no `-local` version since it doesn't make sense to run a performance test locally. Also, note that the private keys are already included, since this configuration will never be run in a "production" setting and it's not important to keep them secret.

## Configuration 3: docker_test_cfg

This configuration is designed to be used when CascadeChain is deployed within a Docker Compose virtual network. The Dockerfile and compose.yaml files included in this repository can be used to create a set of 8 containers, named node0 - node7, that have fixed IP addresses in the 172.16.0.* range, and the per-node configuration folders n0-n7 correspond to those containers. The configuration files should set up the containers with the following layout:

* Primary site: n0-n3
    * PersistentCascadeStore: n0, n2
    * SignatureCascadeStore: n1, n3
    * CascadeMetadataService: n0
* Backup site: n4-n6
    * PersistentCascadeStore: n4
    * SignatureCascadeStore: n5, n6
    * CascadeMetadataService: n4
* Client: n7

Just like in chain_pipeline_cfg, the SignatureCascadeStore nodes at each site will be configured as the replicas of a WanAgent site, and the lowest-numbered node in each site will be the Derecho contact node. In other words, n1 and n3 are the members of WanAgent site 0, n5 and n6 are the members of WanAgent site 1, n0 is the Derecho contact node for the primary site, and n4 is the Derecho contact node for the backup site.

**Configuration files**: Achieving this layout requires each node to load several configuration files. Some of the configuration files are stored in the per-node configuration folders, and some are stored in the parent directory (docker_test_cfg) and symlinked into the per-node folders during the CMake build process; the symlinked files are the same for every node at a site. After building with CMake, each node configuration folder in the output directory will contain the following files:

* derecho_node.cfg
* wanagent.json
* derecho.cfg (*symlink to ../derecho.cfg or ../backup_derecho.cfg depeneding on node*)
* dfgs.json (*symlink to ../dfgs.json or ../backup_dfgs.json depending on node*)
* udl_dlls.cfg (*symlink to ../udl_dlls.cfg or ../backup_udl_dlls.cfg depending on node*)
* layout.json (*symlink to ../layout.json*) **or** backup_layout.json (*symlink to ../backup_layout.json*) depending on node

The symlinked files link to the "primary" version of the file on n0-n3 and n7 (the client), and to the "backup" version of the file on n4-n6. Just like in chain_pipeline_cfg, layout.json is named backup_layout.json on the backup site nodes because the name of this file is not fixed by Derecho or Cascade; it is set in the derecho.cfg file, so the backup site's derecho.cfg specifies that the layout file is named backup_layout.json.

**Generating keys**: CascadeChain requires each site (primary and backup) to have an RSA key pair that is shared among all the replicas at that site. The backup site needs to know the primary site's public key, and the client must know both sites' public keys. To generate these keys, you can run `gen_keys.sh`, which will also copy the PEM files for each key to the correct folder. Specifically, each node folder will contain the following keys after running the script:

* n0-n3: private_key.pem (primary site's private key)
* n4-n6: service_public_key.pem (primary site's public key), private_key.pem (backup site's private key)
* n7: service_public_key.pem (primary site's public key), backup_public_key.pem (backup site's public key), private_key.pem (client node's private key, unused)

Note that `gen_keys.sh` generates a new set of key pairs every time it is run, and copies the key files only within the filesystem of the machine it is run on. This means that if you run this script within a Docker container after launching the Docker Compose configuration, none of the other Docker containers will know about their keys, and you will need to manually copy them (via scp over the virtual Docker network) to the other containers.

To avoid manual copying, you can run the script within the *source* directory (`src/applications/tests/cascade_chain`) on the host machine, rather than within the *output* directory (`build-Debug/src/applications/tests/cascade_chain`) of a Docker container. Since the source directory is copied to each Docker container when it starts, and gets copied again if you run with Compose Watch enabled (i.e. `docker compose up -w`), this will automatically copy all the keys to all the Docker containers. The CMake build process on each Docker container should then copy them from the source directory to the output directory when it copies the other configuration files from the source directory to the output directory.
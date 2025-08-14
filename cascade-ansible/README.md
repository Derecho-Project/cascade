# Cascade Ansible Automation

This repository (`cascade-ansible/`) contains all necessary scripts, templates, and playbooks to **automate the deployment and configuration of CascadeChain nodes using Docker and Ansible**.

Note: please note that the Network used in here is 172.20.0.0 instead of 172.16.0.0 mentioned in compose.yml of this project. the reason is the other project (epollbook) already occupied 172.16.0.0 network for docker. so for isolation i have changed the compose.yml in my system and do the automation accordingly. as per my understanding we just need to change the compose.yml ip block for the convenience. 
---


# Cascade – Ansible‑powered local cluster

This repository automates a multi‑node **CascadeChain** test cluster using **Docker Compose** and **Ansible**.

It gives you:

* One‑shot **first‑time deployment** on a laptop/workstation
* **Template‑driven configs** per node (Jinja2 + `host_vars/all.yml`)
* An always‑on **watcher service** that auto‑rebuilds/restarts on backend code changes and auto‑renders/syncs on config changes

> Target stack: Ubuntu (or WSL), Docker Engine + Compose v2, Ansible.

---

## Repository map (automation bits)

```
cascade-ansible/
  playbooks/
    deploy.yml                # clean, first-time stack bring-up (build + up + start procs)
    build_up.yml              # rebuild images + up -d (no teardown)
    render.yml                # render configs from templates and host_vars/all.yml
    sync_configs.yml          # copy rendered configs into containers
    restart_processes.yml     # (re)start server/client inside containers (idempotent)
    setup_watcher_service.yml # install/enable systemd watcher service
  templates/
    derecho.cfg.j2
    derecho_node.cfg.j2
    wanagent.json.j2
  host_vars/
    all.yml                   # cluster + cfg model (edit this)
  scripts/
    ansible_watch.sh          # file watcher used by the systemd service
  cfg/                        # rendered outputs (n0..n7/{derecho.cfg, derecho_node.cfg, wanagent.json})
```

---

## Prerequisites

Install Docker, Compose, Ansible, and utilities:

```bash
sudo apt-get update
sudo apt-get install -y docker.io docker-compose-plugin ansible inotify-tools
ansible-galaxy collection install community.docker --force
```

If your **Compose build** pulls from private repos (common), set up an SSH agent:

```bash
# create a key if you don't have one
mkdir -p ~/.ssh && chmod 700 ~/.ssh
ssh-keygen -t ed25519 -C "$(whoami)@$(hostname)" -f ~/.ssh/id_ed25519 -N ""

# start agent & load key
eval "$(ssh-agent -s)"
ssh-add ~/.ssh/id_ed25519
# add ~/.ssh/id_ed25519.pub to GitHub → Settings → SSH keys
```

---

## One‑time setup on a new machine

> Run the following from `cascade-ansible/playbooks`.

1. **Clean deploy (first time)**

Build images (BuildKit; uses your SSH agent if present), start containers, generate keys, etc.

```bash
ansible-playbook deploy.yml
```
2. **Render per‑node configs**

```bash
ansible-playbook render.yml
```

3. **Sync configs into containers**

```bash
ansible-playbook sync_configs.yml
```

4. **(Re)start server/client processes**

```bash
ansible-playbook restart_processes.yml
```

5. **Install the always‑on watcher** (systemd)

This keeps the stack up‑to‑date automatically.

```bash
sudo ansible-playbook setup_watcher_service.yml
systemctl status cascade-ansible-watcher     # quick check
```

---

## Day‑to‑day workflow

With the watcher service running, **you don’t run playbooks manually**:

* **Backend code changes** (`src/`, `include/`, `cmake/`, `CMakeLists.txt`, `Dockerfile*`)

  → watcher runs: `build_up.yml` → `restart_processes.yml`

* **Config changes** (`cascade-ansible/templates/` or `cascade-ansible/host_vars/all.yml`)

  → watcher runs: `render.yml` → `sync_configs.yml`

  (run `restart_processes.yml` too if your app needs a restart to pick up changes)

### If the watcher is disabled

* **Code change**:

  ```bash
  ansible-playbook build_up.yml
  ansible-playbook restart_processes.yml
  ```

* **Config change**:

  ```bash
  ansible-playbook render.yml
  ansible-playbook sync_configs.yml
  ansible-playbook restart_processes.yml
  ```

---

## Configuration model

Edit **`cascade-ansible/host_vars/all.yml`** to describe your test topology:

* `nodes:` list with `{ id, ip, site }`
* `contact_ip`, `contact_port`, `restart_leaders`
* RDMA/TCP params: `rdma_provider`, `rdma_domain`
* WAN agent: `wan_private_port`, `sites: [{ id, ips, ports }]`

Templates in `cascade-ansible/templates/` render to **`cascade-ansible/cfg/`**:

```
cfg/
  n0/{derecho.cfg, derecho_node.cfg, wanagent.json}
  n1/{...}
  ...
  n7/{...}
```

`sync_configs.yml` copies each file into the matching container path:

```
/cascade/build-Debug/src/applications/tests/cascade_chain/docker_test_cfg/n{0..7}/
```

> Default compose location used by these playbooks:
> `/home/<user>/cascade/src/applications/tests/cascade_chain/docker_test_cfg/compose.yaml`
>
> If your path differs, update the playbooks’ `repo_path`/`compose_dir` variables accordingly.

---

## Playbook reference

| Playbook                    | When to use                                     |
| --------------------------- | ----------------------------------------------- |
| `deploy.yml`                | First‑time clean install / full reset           |
| `build_up.yml`              | Rebuild images and `up -d` without teardown     |
| `render.yml`                | Re‑render configs from templates/host\_vars     |
| `sync_configs.yml`          | Push rendered configs into running containers   |
| `restart_processes.yml`     | (Re)start servers (nodes 0–6) & client (node 7) |
| `setup_watcher_service.yml` | Install/enable always‑on watcher (systemd)      |

---

## Watcher details

The watcher runs `cascade-ansible/scripts/ansible_watch.sh` under systemd (`cascade-ansible-watcher`).

* **Watches code**: `src/`, `include/`, `cmake/`, `CMakeLists.txt`, `Dockerfile*`
  → triggers `build_up.yml` then `restart_processes.yml`
* **Watches configs**: `cascade-ansible/templates/`, `cascade-ansible/host_vars/all.yml`
  → triggers `render.yml` then `sync_configs.yml`

Logs:

```bash
journalctl -u cascade-ansible-watcher -f
```

Stop/disable:

```bash
sudo systemctl stop cascade-ansible-watcher
sudo systemctl disable cascade-ansible-watcher
```

---

## Useful Docker commands

Set these (optional) to avoid repeating flags:

```bash
export COMPOSE_FILE=~/cascade/src/applications/tests/cascade_chain/docker_test_cfg/compose.yaml
export COMPOSE_PROJECT_NAME=cascade
```

Then:

```bash
docker compose ps -a
docker compose logs -f --tail=200
docker exec -it cascade-node0-1 bash
```

---

## Troubleshooting

* **`invalid empty ssh agent socket: make sure SSH_AUTH_SOCK is set` during build**
  Start an agent and load your key:

  ```bash
  eval "$(ssh-agent -s)"
  ssh-add ~/.ssh/id_ed25519
  ```

  The playbooks also pass `SSH_AUTH_SOCK` to Docker so BuildKit can use it.

* **`restart_processes.yml` says nothing to stop**
  That’s fine. The playbook is idempotent and will (re)start the expected processes.

* **No containers running**
  Run the first-time sequence again: `render.yml` → `deploy.yml` → `sync_configs.yml` → `restart_processes.yml`.

* **Change the cluster IPs/network**
  Update the compose file (authoritative), and keep `host_vars/all.yml` in sync (the `nodes:` IPs and WAN sites).

---

## Clean reset (optional)

```bash
# removes only cascade-node* containers & custom network
docker rm -f $(docker ps -aq --filter="name=cascade-node") || true
docker network rm cascade_derecho || true

# full compose shutdown from project dir
docker compose -f ~/cascade/src/applications/tests/cascade_chain/docker_test_cfg/compose.yaml -p cascade down
```

Then run the first-time sequence again.

---

## Contributing

* Backend changes: edit code under `src/`, `include/`, `cmake/` → watcher rebuilds & restarts.
* Config changes: edit `host_vars/all.yml` or templates → watcher renders & syncs.
* Keep playbooks readable; prefer idempotent tasks.
 playbooks.
3. For advanced customization, edit the Jinja2 templates in templates/ and the playbooks in playbooks/.

# Cascade Ansible Automation

This repository (`cascade-ansible/`) contains all necessary scripts, templates, and playbooks to **automate the deployment and configuration of CascadeChain nodes using Docker and Ansible**.

---

## Directory Structure

cascade-ansible/
├── README.md # ← You are here!
├── host_vars/ # Node-specific configuration variables (YAML)
├── inventories/ # Ansible inventory files
├── output/ # Rendered config files (auto-generated)
├── playbooks/ # Main Ansible playbooks (deploy, render_and_copy_configs, etc)
├── templates/ # Jinja2 templates for all config files


---

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/)
- [Ansible](https://docs.ansible.com/)
- Python 3.x

Install Ansible (if not already):

pip install ansible


#Workflow Overview: 

1. Edit node configurations in host_vars/node0.yml, node1.yml, ..., node7.yml.

2. Render and copy configs: Use Ansible to generate all config files per node and copy them into the appropriate Docker containers.

3. Deploy containers: (If not already running) use the main deploy playbook or your docker-compose setup.



#How to Use:

1. Clone the Repo

git clone <your-private-git-url>
cd cascade/cascade-ansible


2. Initial Deployment (First Time Only)
This will build and start all Docker containers, generate initial configs, and set up everything.

# Deploy and start everything

ansible-playbook -i inventories/local.yml playbooks/deploy.yml

This will:

#Clean up existing containers/networks
#Build Docker images and start all containers
#Perform any initial key/cert generation
#Wait for containers to be healthy

# Change Configuration (Anytime Later)
Whenever you want to update the configuration for any node:

>> Edit the corresponding file in host_vars/

Example:

vim host_vars/node0.yml

>> Render and copy updated configs (no need to restart containers):

cd cascade/cascade-ansible
ansible-playbook -i inventories/local.yml playbooks/render_and_copy_configs.yml

This will:

#Render all templates for every node using the updated YAML files
#Copy the new configs into the correct running containers instantly


Notes

>> All rendered config files are placed in the output/ directory.
>> Make sure Docker and Docker Compose are running before running the playbooks.
>> For advanced customization, edit the Jinja2 templates in templates/ and the playbooks in playbooks/.

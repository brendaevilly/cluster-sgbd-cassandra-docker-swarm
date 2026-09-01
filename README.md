# Cassandra DBMS Cluster with Docker Swarm

A 2-node **Apache Cassandra** cluster deployed with **Docker Swarm**, including a Python script to bulk-load sample user data via CQL. This was built as a seminar/coursework project on distributed database systems (DBMS).

The stack demonstrates a multi-node Cassandra deployment across different Swarm hosts, using `NetworkTopologyStrategy` replication, and shows how to programmatically create a keyspace, create a table, and insert data from a CSV file using the Cassandra Python driver.

## Features

- **2-node Cassandra cluster** (`cassandra1`, `cassandra2`) running Cassandra 5.0, each pinned to a specific Swarm node via placement constraints.
- **Gossip-based cluster discovery** using `GossipingPropertyFileSnitch`, with `cassandra1` acting as the seed node.
- **Resource limits and health checks** configured per service (memory/CPU limits, restart policy, rolling update config).
- **Data loading script** (`inserir_usuarios.py`) that:
  - Connects to the cluster,
  - Creates a `seminario` keyspace with `NetworkTopologyStrategy` (replication factor 2 on `datacenter1`),
  - Creates a `usuarios` (users) table,
  - Bulk-inserts records from `usuarios.csv` (~10,000 sample users with id, email, name, and age).

## Tech Stack

- **Apache Cassandra 5.0**
- **Docker Swarm** (stack deploy, overlay networking, placement constraints)
- **Python 3** with the [`cassandra-driver`](https://github.com/datastax/python-driver)

## Project Structure

```
.
├── cassandra-stack.yml     # Docker Swarm stack definition (2 Cassandra nodes)
├── inserir_usuarios.py     # Script to create keyspace/table and load CSV data
├── usuarios.csv            # Sample user dataset (id, email, name, age)
└── venv/                   # Local Python virtual environment (not meant to be versioned)
```

> **Note:** the `venv/` folder is a local Python virtual environment that ended up committed to the repository. It's not required to run the project and is generally recommended to exclude via `.gitignore`.

## Getting Started

### Prerequisites

- A Docker Swarm cluster with at least two nodes (hostnames `Brenda` and `rais` in the stack file, or adjust the `node.hostname` placement constraints to match your own nodes)
- An existing external overlay network named `cassandra-net`
- Python 3 with `pip`

### 1. Create the overlay network (if it doesn't exist yet)

```bash
docker network create --driver overlay --attachable cassandra-net
```

### 2. Deploy the Cassandra stack

```bash
docker stack deploy -c cassandra-stack.yml cassandra
```

This starts `cassandra1` and `cassandra2` as a single logical cluster (`clusterCanssandra`) in `datacenter1`.

### 3. Load the sample data

Set up a virtual environment and install the Cassandra driver:

```bash
python3 -m venv venv
source venv/bin/activate
pip install cassandra-driver
```

Then run the loading script (from a machine that can resolve/reach the `cassandra1` and `cassandra2` service names — typically from within the same Docker network, or by adjusting `NODES` in the script to the nodes' reachable addresses):

```bash
python inserir_usuarios.py
```

This will:
1. Create the `seminario` keyspace (if it doesn't already exist),
2. Create the `usuarios` table (if it doesn't already exist),
3. Insert all rows from `usuarios.csv` into it.

## Data Model

**Keyspace:** `seminario` — `NetworkTopologyStrategy`, `datacenter1: 2`

**Table:** `usuarios`

| Column | Type | Description       |
|--------|------|--------------------|
| id     | uuid (PK) | Unique user identifier |
| email  | text | User email          |
| nome   | text | User name            |
| idade  | int  | User age              |

## License

This project is licensed under the [MIT License](LICENSE).
<div align="center">

![RushDB Logo](https://raw.githubusercontent.com/Karolstahhcon/rushdb/main/rushdb-logo.svg)

# RushDB

### One database for your records, their relationships, and search over them.

Write JSON. RushDB infers the types, links nested objects into a graph, and
lets you query it with its JSON query language, SQL, Cypher, Gremlin or
Mongo-style documents, plus vector, full-text and hybrid search. One Rust
binary, one data directory, Apache-2.0.


[Website](https://rushdb.com) • [Documentation](https://docs.rushdb.com) • [Manual](docs/manual/README.md) • [RushDB Cloud](https://app.rushdb.com) • [Deploy](deploy/README.md)

**English** • [中文](README_ZH.md) • [日本語](README_JA.md) • [한국어](README_KO.md) • [Deutsch](README_DE.md) • [Français](README_FR.md) • [Português](README_PT.md) • [Español](README_ES.md) • [हिन्दी](README_HI.md) • [العربية](README_AR.md) • [Bahasa Indonesia](README_ID.md) • [ไทย](README_TH.md)

</div>

---

## Quick start

```bash
curl -fsSL https://raw.githubusercontent.com/Karolstahhcon/rushdb/main/install.sh | sh
rushdb up
```

The script installs the `rushdb` binary into `~/.rushdb/bin` after checking
the release checksum (and its Sigstore signature when `cosign` is installed).
`rushdb up` starts a durable node on `http://127.0.0.1:3000` with its data in
`./.rushdb` and the local token `local-token`.

Write a nested JSON document. Every nested object becomes its own record,
linked to its parent:

```bash
export RUSHDB_TOKEN=local-token
curl -s http://127.0.0.1:3000/protocol/v1/execute \
  -H "Authorization: Bearer $RUSHDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"protocolVersion":1,"operation":{"kind":"ingest","ingest":{"kind":"documents","label":"COMPANY",
       "documents":{"name":"Acme","DEPARTMENT":[
         {"name":"Engineering","headcount":12,"EMPLOYEE":[{"name":"Ada","role":"Staff Engineer"},{"name":"Linus","role":"Engineer"}]},
         {"name":"Sales","headcount":4,"EMPLOYEE":[{"name":"Grace","role":"Account Executive"}]}]}}}}'
```

Then find the engineers at Acme. The filter reaches across relationships by
naming the related labels:

```bash
curl -s http://127.0.0.1:3000/protocol/v1/execute \
  -H "Authorization: Bearer $RUSHDB_TOKEN" -H 'Content-Type: application/json' \
  -d '{"protocolVersion":1,"operation":{"kind":"query","query":{"kind":"records","query":{
       "labels":["EMPLOYEE"],
       "predicate":{"role":{"$contains":"Engineer"},"DEPARTMENT":{"COMPANY":{"name":"Acme"}}}}}}}'
```

Continue with the [Quickstart](docs/manual/getting-started/quickstart.md).

## Query it your way

The same records answer every interface.

**SQL** over the PostgreSQL wire protocol, with `psql` or any PostgreSQL
driver. Labels are tables:

```bash
PGPASSWORD=$RUSHDB_TOKEN psql "host=127.0.0.1 port=5432 user=rushdb dbname=rushdb sslmode=disable" \
  -c "SELECT name, headcount FROM department ORDER BY headcount DESC"
```

**Cypher** over HTTP, or over Bolt with the Neo4j drivers
(`rushdb up --languages sql,cypher --bolt-port 7687`):

```cypher
MATCH (d:DEPARTMENT)-->(e:EMPLOYEE)
RETURN d.name AS department, count(e) AS people ORDER BY people DESC
```

**The SDKs** in direct mode (`npm install @rushdb/javascript-sdk`):

```typescript
import RushDB from '@rushdb/javascript-sdk'

const db = new RushDB(process.env.RUSHDB_TOKEN, { direct: true, engineUrl: 'http://127.0.0.1:3000' })

const engineers = await db.records.find({
  labels: ['EMPLOYEE'],
  where: { role: { $contains: 'Engineer' }, DEPARTMENT: { COMPANY: { name: 'Acme' } } },
  orderBy: { name: 'asc' }
})
console.log(engineers.data.map((employee) => employee.data.name)) // [ 'Ada', 'Linus' ]
```

Gremlin, SQL/PGQ `GRAPH_TABLE`, Mongo-style documents and ISO GQL use the
same HTTP endpoint. See [Query languages](docs/manual/languages/README.md).

## What's in the box

| Capability | What it means |
|---|---|
| **No schema to design** | Write any JSON shape. Types are inferred per value, nested objects become linked records, and the engine keeps an exact catalog of labels, properties and relationships that you can query. |
| **Properties are first-class** | The Labeled Meta-Property Graph (LMPG) stores each property as a typed definition shared by the records that use it, so schema discovery, value ranges and facets come from the storage layout itself. |
| **Many query languages** | A native JSON query language, SQL, Gremlin, SQL/PGQ, Mongo-style documents, and openCypher and ISO GQL as release candidates. |
| **Search** | Vector indexes (exact and HNSW) over the vectors you supply, BM25 full-text indexes, and hybrid search that fuses both. |
| **Durable and transactional** | A write-ahead log, MVCC snapshots, multi-statement transactions, backups with point-in-time restore, and streaming replication to read-only followers. |
| **Standard protocols** | HTTP, the PostgreSQL wire protocol, gRPC and Bolt from one process. |
| **Multitenancy** | Optional fail-closed tenants, each with its own keyspace. |
| **One binary** | `rushdb serve` runs the server; `rushdb up` runs a local node. No other service to operate. |

## Deploy

| Where | How |
|---|---|
| Docker | `docker run -p 127.0.0.1:3000:3000 -v rushdb:/data -e RUSHDB_TOKEN=change-me rushdb/rushdb` |
| Docker Compose | [`deploy/templates/docker-compose`](deploy/templates/docker-compose/), with an HTTPS variant behind Caddy |
| Kubernetes | [Helm chart](charts/rushdb/): `helm install rushdb oci://ghcr.io/Karolstahhcon/charts/rushdb --set auth.token=...` |
| Linux VM | [cloud-init](deploy/templates/cloud-init/) for DigitalOcean, Hetzner, AWS, GCP, Azure; [systemd unit](deploy/templates/systemd/) |
| Platforms | [Fly.io](deploy/templates/fly/), [Render](deploy/templates/render/), [Coolify](deploy/templates/coolify/), [Dokploy](deploy/templates/dokploy/) |


### Other ways to install

| Method | Command | Notes |
|---|---|---|
| Install script | `curl -fsSL https://raw.githubusercontent.com/Karolstahhcon/rushdb/main/install.sh \| sh` | Linux (glibc 2.28+) and macOS, x86-64 and arm64. `sh -s -- --version v3.0.0` pins a release. |
| Homebrew | `brew install Karolstahhcon/tap/rushdb` | macOS and Linux. `brew services start rushdb` runs it in the background. |
| Docker | `rushdb/rushdb` | `linux/amd64` and `linux/arm64`; also `ghcr.io/Karolstahhcon/rushdb`. |
| Cargo | `cargo binstall rushdb-server` or `cargo install rushdb-server --locked` | `binstall` fetches the release binary; `install` compiles (needs a C++ compiler). |
| Release archive | GitHub releases | Archives with SHA-256 checksums, an SBOM, and Sigstore signatures. |
| Source | `cargo build --release -p rushdb-server --bin rushdb` | See [Installation](docs/manual/getting-started/installation.md). |

Windows is not supported natively; use WSL 2 or Docker. Read

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


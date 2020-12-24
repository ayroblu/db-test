# Database testing for understanding performance

Databases are magic, and there's a lot of random theory out there
The purpose of this is to figure out what real world penalties there are from different types of data models

## Types of tests
### Isolation
Databases provide different isolation levels serializable, repeatable read / snapshopt isolation and read commited for example

### Throughput
|----------------|------------------------------------------------|
| Setup          | Description                                    |
|----------------|------------------------------------------------|
| No setup       | Insert in a key value                          |
| No setup       | Insert in a glossary style with no indexes     |
| No setup       | Insert in a glossary style with indexes on all |
| 1 million rows | Insert in a glossary style with no indexes     |
| 1 million rows | Insert in a glossary style with indexes on all |
| No setup       | Insert in a 1000 column style with no indexes  |
| No setup       | Insert in a 1000 column style with indexes     |
| 1 million rows | Insert in a 1000 column style with no indexes  |
| 1 million rows | Insert in a 1000 column style with indexes     |
|----------------|------------------------------------------------|

### Setup Postgres
- Run a docker command

```bash
docker run --name posttest -d -p 5432:5432 -e POSTGRES_PASSWORD=todo -e POSTGRES_USER=docker postgres:alpine
```

## Use cases of different database types

Cassandra

- Write heavy workload, such as time series sensor data

Relational

- If you need to read before write - relational integrity

### Common use cases

- Account / auth table, for users, orgs, with passwords etc
  - Generally only one person writing, unlikely to have one person doing two things at once
  - Good for Cassandra
- Blog, where a few people write, a lot of people read
  - Show error if someone edits the same article at the same time, somewhat undefined for race conditions, still okay for cassandra
- Kanban/todo application
  - What happens if we both try to move the same task? This seems bad?
- Social media, a lot of writes and reads
  - At some point my post goes up, enough consistency to read your writes but you don't need that much
  - Things like counters are hard
- Chat app
  - Ordering of messages - on server received time, seems okay for cassandra? Rarely do two people want to update the same message.
- Dashboard
  - Performs a read heavy workload, more a question of RAM and offline processing power

### Thoughts on "consistency"

How do you do clocks? For example, if your clock is wrong and it gets corrected by a few milliseconds, does it start writing overlapping timestamps?

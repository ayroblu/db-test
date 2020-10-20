# Database testing for understanding performance

Databases are magic, and there's a lot of random theory out there
The purpose of this is to figure out what real world penalties there are from different types of data models

## Types of tests
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

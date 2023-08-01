# Clickhouse Data Connector

This repository contains the source code for a prototype
[data connector agent](https://github.com/hasura/graphql-engine/blob/master/dc-agents/README.md) for Clickhouse to be
able to use it with Hasura.

This repository also contains a Dockerfile to be able to build an image in your own architecture.

To use the Clickhouse connector with Hasura:

- Deploy the connector somewhere that is accessible to Hasura
- In the Hasura console, add a new data connector called "clickhouse" pointing it to your deployed agent.
- Add a new database in the console, where you should now see "clickhouse" as a database type.
- Add your clickhouse configuration:
  - host: Clickhouse host URL
  - scheme: http/https
  - API key: Clickhouse API key for authentication, pass any value if your database is unauthenticated

Please note that this is only a prototype and may contain bugs, reliability, and performance issues.

## Deploy to Hasura Cloud

You can use the new [Hasura Data Connector Plugin](https://hasura.io/docs/latest/hasura-cli/connector-plugin/) for the
Hasura CLI to deploy this connector to Hasura Cloud.

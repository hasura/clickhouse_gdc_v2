# Clickhouse Data Connector

This repository contains the source code for a
[data connector agent](https://github.com/hasura/graphql-engine/blob/master/dc-agents/README.md) allow the use of [Clickhouse](https://clickhouse.com/) databases as data sources for Hasura v2.

This repository also contains a Dockerfile to be able to build an image in your own architecture.

## Status

This is still in active development, and should not be considered stable.

In terms of feature coverage, this project is currently feature complete.

Please report any bugs by filing issues in this repo.

### Known issues

- Column comparison operators are not supported, but Hasura GraphQL Engine will still allow them, resulting in runtime errors.
- We are using the clickhouse http interface. We will move to a tcp client in due time.
- No support for mutations or subscriptions. This may change in the future.
- Table columns with complext types are not supported

## Using this connector

1. Deploy the connector somewhere that is accessible to Hasura. See below for instructions on deploying to Hasura Cloud.
2. [Add the agent to your hasura instance](https://hasura.io/docs/latest/databases/data-connectors/#adding-hasura-graphql-data-connector-agent-to-metadata).
3. Add a data source of type "Hasura v2 Clickhouse". You will need to provide the clickhouse database username, password, and url.

## Deploy to Hasura Cloud

You can use the new [Hasura Data Connector Plugin](https://hasura.io/docs/latest/hasura-cli/connector-plugin/) for the
Hasura CLI to deploy this connector to Hasura Cloud.

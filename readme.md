# Clickhouse Data Connector

This repository contains the source code for a
[data connector agent](https://github.com/hasura/graphql-engine/blob/master/dc-agents/README.md) allow the use of [Clickhouse](https://clickhouse.com/) databases as data sources for Hasura v2.

This repository also contains a Dockerfile to be able to build an image in your own architecture.

## Status

The project is currently feature complete, but may not be stable.
Please report any bugs by filing issues in this repo.

### Known issues & limitations

- Column comparison operators are not supported, but Hasura GraphQL Engine will still allow them, resulting in runtime errors.
- We are using the clickhouse http interface. We will move to a tcp client in due time.
- No support for mutations or subscriptions.
- Table columns with complex types are supported as JSON strings, not full graphql types.
- There is a currently a bug where the console UI will demand at least one entry for the tables array, and one entry for that table's columns. This should soon be fixed. For the time being, you may work around this by providing dummy values for both table an column names and aliases.

## Using this connector

1. Deploy the connector somewhere that is accessible to Hasura. See below for instructions on deploying to Hasura Cloud.
2. [Add the agent to your hasura instance](https://hasura.io/docs/latest/databases/data-connectors/#adding-hasura-graphql-data-connector-agent-to-metadata).
3. Add a data source of type "Hasura v2 Clickhouse". You will need to provide the clickhouse database username, password, and url.
4. Providing aliases: table and column names should be valid [GraphQL names](https://spec.graphql.org/October2021/#sec-Names). You may provide aliases for any table or column names that are not valid GraphQL names. You do not need to specify all tables or columns.

## Deploy to Hasura Cloud

You can use the new [Hasura Data Connector Plugin](https://hasura.io/docs/latest/hasura-cli/connector-plugin/) for the
Hasura CLI to deploy this connector to Hasura Cloud.

## Using environment variables for secrets

Setting username and password in the UI will result in these values being stored in your metadata.
This may be undesirable if your metadata is being stored in a git repository for example.

To use environment variables, you can follow these steps:

0. Configure your data source as per the above instructions. This may mean temporarily inputing your password into the UI.
1. Create environment variables for any of username, password, url. On hasura cloud you will do this from the dashboard.
2. Export your metadata json file from the console settings. We will be editing this file.
3. Locate your data source configuration within the file. This will be under `metadata.sources[n].configuration`, where sources is an array.
4. Set the template key in the configuration object to the string template below:

```json
{
  "template": "{\"password\":{{$env?[$config.password] ?? $config.password}},\"url\": {{$env?[$config.url] ?? $config.url}},\"username\": {{$env?[$config.username] ?? $config.username}},\"tables\":{{$config?.tables}}}",
  "timeout": null,
  "value": {
    /* config ommited */
  }
}
```

5. The template checks if the value of url, username, and password are also the names of environment variables. If that is true, the value of the environment variable is used. If not, default to the username, password, url values in config.
6. To set the url from an environment variable (Optional): in the configuration value, update the `url` key to the name of the env var you wish to use.
7. To set the username from an environment variable (Optional): in the configuration value, update the `username` key to the name of the env var you wish to use.
8. To set the password from an environment variable (Optional): in the configuration value, update the `passwird` key to the name of the env var you wish to use.
9. Finally, save the file, and import this metadata to your project from the UI.
10. Important: If you update your configuration from the UI, **the template will be lost and you will need to follow the above steps again**. We are working to resolve this issue.

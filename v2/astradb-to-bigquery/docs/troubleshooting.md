# Troubleshooting AstraDB Template Issues

## Table of content

- [Initialization Issues](#initialization-issues)
  - [Symptom: Google Secret Cannot be found](#symptom-google-secrets-cannot-be-found)
  - [Symptom: Invalid Astra Token or Database Identifiers](#symptom-invalid-astra-token-or-secure-connect-bundle)
- [AstraDB Related issues](#astradb-related-issues)
  - [Symptom: Cannot connect DB](#symptom-cannot-connect-db)
  - [Symptom: Keyspace does not exist](#symptom-keyspace-does-not-exist)
  - [Symptom: Table does not exist](#symptom-table-does-not-exist)
  - [Symptom: Schema Generation error](#symptom-schema-generation-error)

## Initialization Issues

### Symptom: Google Secrets Cannot be found

For parameter `Astra token` it is possible to provide either a valid token _Starting with AstraCS:..._ or a 
the resource id of the secret in the Google Secret Manager. If you provide a secret and got an error message 
check the following:

**Check that the required secret existx and can be access by the dataflow user.**

- ✅ Check that the secrets are created in the same project as the Dataflow Job
- ✅ Check that the full path has been provided including the version number
- ✅ Check Permissions of the secrets for visibility
- ✅ Check Visibility and regions of the Dataflow JOb and Secret Manager

### Symptom: Invalid Astra Token or Database Identifiers

**Check that the token not being revoked and got enough permission**

## AstraDB Related issues 

> _Installing the [Astra CLI](https://awesome-astra.github.io/docs/pages/astra/astra-cli/) is recommended to help with troubleshooting_

### Symptom: Cannot connect DB

**Check that the DB exists and is not hibernating**

✅ Check that your source database exists from the Astra User interface or using the Astra CLI (https://awesome-astra.github.io/docs/pages/astra/astra-cli/)

```bash
astra db list
astra db describe <database-name>
```

✅ If the database is hibernating, to resume it checking this [documentation](https://awesome-astra.github.io/docs/pages/astra/resume-db/)

```roomsql
astra db resume <database-name>
```

### Symptom: Keyspace does not exist

**Keyspace name provided is invalid, check its value and validate keyspace exists o create it**

✅ Creating a keyspace if not exist

```bash
astra db list-keyspace <database-name>
astra db create-keyspace <database-name> -k <keyspace-name>
```

### Symptom: Table does not exist

**Source table name is incorrect or table does not exist. You will need to create the proper table and populate with data first.**

✅ Creating a table if not exists.

```bash
astra db cqlsh <database-name> \
   -k <keyspace-name> \
   -e "CREATE TABLE IF NOT EXISTS <table-name> (id text PRIMARY KEY, name text, age int);"
```

### Symptom: Schema Generation error

**Check the schema of the source table and compatiblity**:
- UDT are not supported
- Tuple are not supported
- Complex nested structures are not supported

```bash
astra db cqlsh <database-name> \
   -k <keyspace-name> \
   -e "describe table <table-name>;"
```



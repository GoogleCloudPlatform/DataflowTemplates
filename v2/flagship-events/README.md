# Flagship Events Dataflow Job

## Build and Deploy

### Build the Uber Jar

In order to have the Conscrypt jar packaged you will need to build the Uber Jar first
```sh
mvn clean package
```

Then you can deploy to a given environment by calling the deploy script with one of "intg", "stge" or "prod":
```sh
./bin/deploy-flagship-events.sh {env}
```

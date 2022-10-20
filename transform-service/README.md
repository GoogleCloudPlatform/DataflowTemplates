# Syndeo Transform Service

This project defines a prototype service to serve as 'Transform Service' for
syndeo.

A transform service is a stateless service that provides the following APIs:

- `listTransforms` - This API call returns a list of transforms that are
    valid and available to be launched in Syndeo.
- `validateTransform` - This API receives a transform configuration and an
    input schema, and it validates whether the configuration and input
    can be used for the given transform.
    This function can also return the schema of the output `PCollection`s
    that would be returned by the input transform.

This prototype aims to validate whether a Java-based service that runs on
Google Cloud can be a valid process for Syndeo.

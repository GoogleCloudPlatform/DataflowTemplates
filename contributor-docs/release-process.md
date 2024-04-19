# Release Process

Templates are released in a weekly basis (best-effort) as part of the efforts to
keep [Google-provided Templates](https://cloud.google.com/dataflow/docs/guides/templates/provided-templates) updated with latest fixes and improvements.

In case desired, you can stage and use your own changes using the `Staging (Deploying) Templates` steps.

To execute the release of multiple templates, we provide a single Maven command to release Templates, which is a shortcut to
stage all templates while running additional validations.

```shell
mvn clean verify -PtemplatesRelease \
  -DprojectId="{projectId}" \
  -DbucketName="{bucketName}" \
  -DlibrariesBucketName="{bucketName}-libraries" \
  -DstagePrefix="$(date +%Y_%m_%d)-00_RC00"
```

## Maven artifacts

As part of the Templates development process, we release the common artifact snapshots to Maven Central, not
modules that contain finalized templates. This allows users to consume those resources and modules without forking the
entire project, while keeping artifacts at a reasonable size.

In order to release artifacts, `~/.m2/settings.xml` should be configured to contain Sonatype's username and password:

```xml

<servers>
  <server>
    <id>ossrh</id>
    <username>(user)</username>
    <password>(password)</password>
  </server>
</servers>
```

And the command to release (for example, the development plugin and Spanner together):

```shell
mvn clean deploy -am -Prelease \
  -pl plugins/templates-maven-plugin \
  -pl v2/spanner-common
```

If you intend to use those resources in an external project, your `pom.xml` should include:

```xml

<repositories>
  <repository>
    <id>ossrh</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </repository>
</repositories>
```

```xml

<pluginRepositories>
  <pluginRepository>
    <id>ossrh</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots</url>
  </pluginRepository>
</pluginRepositories>
```
package com.google.cloud.teleport.v2.templates;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TransactionRunner.TransactionCallable;
import com.google.cloud.spanner.Value;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequence;
import com.google.cloud.teleport.v2.templates.datastream.ChangeEventSequenceFactory;
import org.junit.Test;

public class SpannerExceptionClassifierIT {

  String project = "";
  String instance = "";
  String db = "";

  @Test
  public void testInterleaveError() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Books")
                    .set("id").to(4)
                    .set("author_id").to(100)
                    .set("title").to("Child")
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testFKError() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("C1")
                    .set("id1").to(4)
                    .set("id2").to(4)
                    // .set("author_id").to(100)
                    // .set("title").to("Child")
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testUniqueIndexError() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Authors")
                    .set("author_id").to(4)
                    .set("name").to("Jane Austen")
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testCheckConstraintError() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Authors")
                    .set("author_id").to(-1)
                    .set("name").to("ABCD")
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testTableNotFound() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("AAAA")
                    .set("author_id").to(1)
                    .set("name").to("ABCD")
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testColumnNotFound() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Authors")
                    .set("author_id").to(5)
                    .set("Unknown_column").to("ABCD")
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testDeleteParentWhenChildExists() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.delete("Authors", Key.of(1));

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void testUniqueIndexNullValue() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Authors")
                    .set("author_id").to(6)
                    .set("name").to(Value.string(null))
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void datatypeMismatch() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Books")
                    .set("id").to(1.5)
                    .set("author_id").to(1)
                    .set("title").to("New Book")
                    .build();


                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void nullInNonNullColumn() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Books")
                    .set("id").to(6)
                    .set("author_id").to(1)
                    .set("title").to(Value.string(null))
                    .build();


                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void writeToGeneratedColumn() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("Books")
                    .set("id").to(6)
                    .set("author_id").to(1)
                    .set("title").to("New Book")
                    .set("titleUpperStored").to("NEW BOOK")
                    .build();


                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void genPKWithoutDependentCols() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newUpdateBuilder("genPK")
                    .set("id1").to(6)
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void deleteFromGenPKTable() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.delete("genPK", Key.of(6));

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void multipleValuesForColumn() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newInsertBuilder("genPK")
                    .set("id1").to(6)
                    .set("part1").to(10)
                    .set("part2").to(20)
                    .set("part2").to(30)
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void deleteFromFKParentTable() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.delete("FKP", Key.of(1));

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void updateParentFKTable() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newUpdateBuilder("FKP1")
                    .set("id").to(1)
                        .set("title").to(Value.string(null))
                        .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  @Test
  public void insertWithPartialKey() {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder().setProjectId(project);
    Spanner spanner = builder.build().getService();
    DatabaseClient client = spanner.getDatabaseClient(
        DatabaseId.of(project, instance, db));

    try {
      client.readWriteTransaction().run(
          (TransactionCallable<Void>)
              transaction -> {
                Mutation mutation = Mutation.newUpdateBuilder("interleaveC1")
                    .set("id1").to(1)
                    .set("id5").to(2)
                    .set("id6").to(3)
                    .build();

                // Apply shadow and data table mutations.
                transaction.buffer(mutation);
                return null;
              });
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}

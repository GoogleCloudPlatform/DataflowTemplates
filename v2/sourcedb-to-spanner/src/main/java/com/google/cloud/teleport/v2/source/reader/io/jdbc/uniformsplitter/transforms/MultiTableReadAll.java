/*
 * Copyright (C) 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.transforms;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.beam.sdk.util.Preconditions.checkArgumentNotNull;
import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import com.google.auto.value.AutoValue;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableIdentifier;
import com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.TableReadSpecification;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import javax.annotation.Nullable;
import javax.sql.DataSource;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.DataSourceProviderFromDataSourceConfiguration;
import org.apache.beam.sdk.io.jdbc.JdbcIO.PreparedStatementSetter;
import org.apache.beam.sdk.io.jdbc.JdbcIO.RowMapper;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.transforms.display.HasDisplayData;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.TypeDescriptors.TypeVariableExtractor;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link PTransform} that reads from multiple database tables in parallel using a single, generic
 * Dataflow graph.
 *
 * <p>Unlike {@link JdbcIO#readAll()}, this transform is designed to handle heterogeneous tables
 * with different schemas, {@link RowMapper}s, and {@link QueryProvider}s. It takes a {@link
 * PCollection} of parameter elements (typically {@link
 * com.google.cloud.teleport.v2.source.reader.io.jdbc.uniformsplitter.range.Range} objects),
 * identifies their source table, and executes the appropriate SQL query dynamically.
 *
 * @param <ParameterT> the type of the input parameter elements.
 * @param <OutputT> the type of the data read from the database.
 */
@AutoValue
public abstract class MultiTableReadAll<ParameterT, OutputT>
    extends PTransform<PCollection<ParameterT>, PCollection<OutputT>> {
  private static final Logger LOG = LoggerFactory.getLogger(MergeRangesDoFn.class);
  private static final boolean DEFAULT_DISABLE_AUTO_COMMIT = true;

  @Pure
  protected abstract @Nullable SerializableFunction<Void, DataSource> getDataSourceProviderFn();

  @Pure
  protected abstract @Nullable ValueProvider<QueryProvider> getQueryProvider();

  @Pure
  protected abstract @Nullable PreparedStatementSetter<ParameterT> getParameterSetter();

  @Pure
  protected abstract ImmutableMap<TableIdentifier, TableReadSpecification<OutputT>>
      getTableReadSpecifications();

  @Pure
  protected abstract SerializableFunction<ParameterT, TableIdentifier> getTableIdentifierFn();

  @Pure
  protected abstract @Nullable Coder<OutputT> getCoder();

  protected abstract boolean getOutputParallelization();

  protected abstract boolean getDisableAutoCommit();

  protected abstract Builder<ParameterT, OutputT> toBuilder();

  /**
   * Returns a new builder for {@link MultiTableReadAll}.
   *
   * @param <ParameterT> input type.
   * @param <OutputT> output type.
   * @return a builder instance.
   */
  public static Builder builder() {
    return new AutoValue_MultiTableReadAll.Builder()
        .setDisableAutoCommit(DEFAULT_DISABLE_AUTO_COMMIT);
  }

  @AutoValue.Builder
  abstract static class Builder<ParameterT, OutputT> {
    abstract Builder<ParameterT, OutputT> setDataSourceProviderFn(
        SerializableFunction<Void, DataSource> dataSourceProviderFn);

    abstract Builder<ParameterT, OutputT> setQueryProvider(ValueProvider<QueryProvider> query);

    abstract Builder<ParameterT, OutputT> setParameterSetter(
        PreparedStatementSetter<ParameterT> parameterSetter);

    abstract Builder<ParameterT, OutputT> setTableReadSpecifications(
        ImmutableMap<TableIdentifier, TableReadSpecification<OutputT>> tableReadSpecifications);

    abstract Builder<ParameterT, OutputT> setTableIdentifierFn(
        SerializableFunction<ParameterT, TableIdentifier> tableIdentifierFn);

    abstract Builder<ParameterT, OutputT> setCoder(Coder<OutputT> coder);

    abstract Builder<ParameterT, OutputT> setOutputParallelization(boolean outputParallelization);

    abstract Builder<ParameterT, OutputT> setDisableAutoCommit(boolean disableAutoCommit);

    abstract MultiTableReadAll<ParameterT, OutputT> build();
  }

  /**
   * Configures the data source for the read operation.
   *
   * @param config the data source configuration.
   * @return a new transform instance with the data source configured.
   */
  public MultiTableReadAll<ParameterT, OutputT> withDataSourceConfiguration(
      DataSourceConfiguration config) {
    return withDataSourceProviderFn(DataSourceProviderFromDataSourceConfiguration.of(config));
  }

  /**
   * Configures a provider function for the data source.
   *
   * @param dataSourceProviderFn the data source provider function.
   * @return a new transform instance.
   */
  public MultiTableReadAll<ParameterT, OutputT> withDataSourceProviderFn(
      SerializableFunction<Void, DataSource> dataSourceProviderFn) {
    if (getDataSourceProviderFn() != null) {
      throw new IllegalArgumentException(
          "A dataSourceConfiguration or dataSourceProviderFn has "
              + "already been provided, and does not need to be provided again.");
    }
    return toBuilder().setDataSourceProviderFn(dataSourceProviderFn).build();
  }

  /**
   * Sets a static query to be used for all input elements.
   *
   * @param query the SQL query string.
   * @return a new transform instance.
   */
  public MultiTableReadAll<ParameterT, OutputT> withQuery(String query) {
    checkArgument(query != null, "JdbcIO.readAll().withQuery(query) called with null query");
    return withQueryProvider((_unused) -> query);
  }

  /**
   * Configures a {@link QueryProvider} to dynamically generate queries for each input element.
   *
   * @param queryProvider the query provider instance.
   * @return a new transform instance.
   */
  public MultiTableReadAll<ParameterT, OutputT> withQueryProvider(QueryProvider queryProvider) {
    checkArgument(
        queryProvider != null, "withQueryProvider(queryProvider) called with null queryProvider");
    return withQueryProvider(ValueProvider.StaticValueProvider.of(queryProvider));
  }

  /**
   * Configures a {@link ValueProvider} for the {@link QueryProvider}.
   *
   * @param queryProvider the query provider value provider.
   * @return a new transform instance.
   */
  public MultiTableReadAll<ParameterT, OutputT> withQueryProvider(
      ValueProvider<QueryProvider> queryProvider) {
    checkArgument(
        queryProvider != null, "withQueryProvider(queryProvider) called with null queryProvider");
    return toBuilder().setQueryProvider(queryProvider).build();
  }

  /**
   * Sets the {@link PreparedStatementSetter} to set the parameters of the query for each input
   * element.
   *
   * <p>For example,
   *
   * <pre>{@code
   * JdbcIO.<String, Row>readAll()
   *     .withQuery("select * from table where field = ?")
   *     .withParameterSetter((element, preparedStatement) -> preparedStatement.setString(1, element))
   * }</pre>
   */
  public MultiTableReadAll<ParameterT, OutputT> withParameterSetter(
      PreparedStatementSetter<ParameterT> parameterSetter) {
    checkArgumentNotNull(
        parameterSetter,
        "JdbcIO.readAll().withParameterSetter(parameterSetter) called "
            + "with null statementPreparator");
    return toBuilder().setParameterSetter(parameterSetter).build();
  }

  /**
   * Configures the per-table read specifications.
   *
   * @param tableReadSpecifications a map from {@link TableIdentifier} to {@link
   *     TableReadSpecification}.
   * @return a new transform instance.
   */
  public MultiTableReadAll<ParameterT, OutputT> withTableReadSpecifications(
      ImmutableMap<TableIdentifier, TableReadSpecification<OutputT>> tableReadSpecifications) {
    return toBuilder().setTableReadSpecifications(tableReadSpecifications).build();
  }

  /**
   * Sets the function used to extract the {@link TableIdentifier} from an input element.
   *
   * @param tableIdentifierFn the table identifier extraction function.
   * @return a new transform instance.
   */
  public MultiTableReadAll<ParameterT, OutputT> withTableIdentifierFn(
      SerializableFunction<ParameterT, TableIdentifier> tableIdentifierFn) {
    return toBuilder().setTableIdentifierFn(tableIdentifierFn).build();
  }

  /**
   * @deprecated
   *     <p>{@link JdbcIO} is able to infer appropriate coders from other parameters.
   */
  @Deprecated
  public MultiTableReadAll<ParameterT, OutputT> withCoder(Coder<OutputT> coder) {
    checkArgument(coder != null, "JdbcIO.readAll().withCoder(coder) called with null coder");
    return toBuilder().setCoder(coder).build();
  }

  /**
   * Whether to reshuffle the resulting PCollection so results are distributed to all workers. The
   * default is to parallelize and should only be changed if this is known to be unnecessary.
   */
  public MultiTableReadAll<ParameterT, OutputT> withOutputParallelization(
      boolean outputParallelization) {
    return toBuilder().setOutputParallelization(outputParallelization).build();
  }

  /**
   * Whether to disable auto commit on read. Defaults to true if not provided. The need for this
   * config varies depending on the database platform. Informix requires this to be set to false
   * while Postgres requires this to be set to true.
   */
  public MultiTableReadAll<ParameterT, OutputT> withDisableAutoCommit(boolean disableAutoCommit) {
    return toBuilder().setDisableAutoCommit(disableAutoCommit).build();
  }

  @VisibleForTesting
  protected @Nullable Coder<OutputT> inferCoder(
      CoderRegistry registry, SchemaRegistry schemaRegistry) {
    if (getCoder() != null) {
      return getCoder();
    } else {
      RowMapper<OutputT> rowMapper =
          getTableReadSpecifications().values().asList().get(0).rowMapper();
      TypeDescriptor<OutputT> outputType =
          TypeDescriptors.extractFromTypeParameters(
              rowMapper,
              RowMapper.class,
              new TypeVariableExtractor<RowMapper<OutputT>, OutputT>() {});
      try {
        return schemaRegistry.getSchemaCoder(outputType);
      } catch (NoSuchSchemaException e) {
        LOG.info(
            "Unable to infer a schema for type {}. Attempting to infer a coder without a schema.",
            outputType);
      }
      try {
        return registry.getCoder(outputType);
      } catch (CannotProvideCoderException e) {
        LOG.warn("Unable to infer a coder for type {}", outputType);
        return null;
      }
    }
  }

  @Override
  public PCollection<OutputT> expand(PCollection<ParameterT> input) {
    Coder<OutputT> coder =
        inferCoder(input.getPipeline().getCoderRegistry(), input.getPipeline().getSchemaRegistry());
    checkStateNotNull(
        coder,
        "Unable to infer a coder for JdbcIO.readAll() transform. "
            + "Provide a coder via withCoder, or ensure that one can be inferred from the"
            + " provided RowMapper.");
    PCollection<OutputT> output =
        input
            .apply(
                ParDo.of(
                    new MultiTableReadFn<>(
                        checkStateNotNull(getDataSourceProviderFn()),
                        checkStateNotNull(getQueryProvider()),
                        checkStateNotNull(getParameterSetter()),
                        getTableReadSpecifications(),
                        getTableIdentifierFn(),
                        getDisableAutoCommit())))
            .setCoder(coder);

    if (getOutputParallelization()) {
      output = output.apply(new Reparallelize<>());
    }

    try {
      TypeDescriptor<OutputT> typeDesc = coder.getEncodedTypeDescriptor();
      SchemaRegistry registry = input.getPipeline().getSchemaRegistry();
      Schema schema = registry.getSchema(typeDesc);
      output.setSchema(
          schema,
          typeDesc,
          registry.getToRowFunction(typeDesc),
          registry.getFromRowFunction(typeDesc));
    } catch (NoSuchSchemaException e) {
      // ignore
    }

    return output.apply("SetToProcessingTime", ParDo.of(new TimeStampRow<>()));
  }

  @Override
  public void populateDisplayData(DisplayData.Builder builder) {
    super.populateDisplayData(builder);
    /* TODO, make this more meaningful with list of queries or tables possibly */
    builder.add(DisplayData.item("query", getQueryProvider()));

    if (!getTableReadSpecifications().isEmpty()) {
      builder.add(
          DisplayData.item(
              "rowMapper",
              getTableReadSpecifications()
                  .values()
                  .asList()
                  .get(0)
                  .rowMapper()
                  .getClass()
                  .getName()));
    }
    if (getCoder() != null) {
      builder.add(DisplayData.item("coder", getCoder().getClass().getName()));
    }
    if (getDataSourceProviderFn() instanceof HasDisplayData) {
      ((HasDisplayData) getDataSourceProviderFn()).populateDisplayData(builder);
    }
  }

  /**
   * An interface used by the JdbcIO {@link MultiTableReadAll} to get the read Query for a given
   * table.
   */
  @FunctionalInterface
  public interface QueryProvider<T> extends Serializable {
    String getQuery(T element) throws Exception;
  }
}

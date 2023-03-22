/*
 * Copyright (C) 2021 Google LLC
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
package com.google.api.services.dataplex.v1;

/**
 * Service definition for CloudDataplex (v1).
 *
 * <p>Dataplex API is used to manage the lifecycle of data lakes.
 *
 * <p>For more information about this service, see the <a
 * href="https://cloud.google.com/dataplex/docs" target="_blank">API Documentation</a>
 *
 * <p>This service uses {@link CloudDataplexRequestInitializer} to initialize global parameters via
 * its {@link Builder}.
 *
 * @since 1.3
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public class CloudDataplex
    extends com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient {

  // Note: Leave this static initializer at the top of the file.
  static {
    com.google.api.client.util.Preconditions.checkState(
        (com.google.api.client.googleapis.GoogleUtils.MAJOR_VERSION == 1
                && (com.google.api.client.googleapis.GoogleUtils.MINOR_VERSION >= 32
                    || (com.google.api.client.googleapis.GoogleUtils.MINOR_VERSION == 31
                        && com.google.api.client.googleapis.GoogleUtils.BUGFIX_VERSION >= 1)))
            || (com.google.api.client.googleapis.GoogleUtils.MAJOR_VERSION > 1),
        "You are currently running with version %s of google-api-client. "
            + "You need at least version 1.31.1 of google-api-client to run version "
            + "1.32.1 of the Cloud Dataplex API library.",
        com.google.api.client.googleapis.GoogleUtils.VERSION);
  }

  /**
   * The default encoded root URL of the service. This is determined when the library is generated
   * and normally should not be changed.
   *
   * @since 1.7
   */
  public static final String DEFAULT_ROOT_URL = "https://dataplex.googleapis.com/";

  /**
   * The default encoded mTLS root URL of the service. This is determined when the library is
   * generated and normally should not be changed.
   *
   * @since 1.31
   */
  public static final String DEFAULT_MTLS_ROOT_URL = "https://dataplex.mtls.googleapis.com/";

  /**
   * The default encoded service path of the service. This is determined when the library is
   * generated and normally should not be changed.
   *
   * @since 1.7
   */
  public static final String DEFAULT_SERVICE_PATH = "";

  /**
   * The default encoded batch path of the service. This is determined when the library is generated
   * and normally should not be changed.
   *
   * @since 1.23
   */
  public static final String DEFAULT_BATCH_PATH = "batch";

  /**
   * The default encoded base URL of the service. This is determined when the library is generated
   * and normally should not be changed.
   */
  public static final String DEFAULT_BASE_URL = DEFAULT_ROOT_URL + DEFAULT_SERVICE_PATH;

  /**
   * Constructor.
   *
   * <p>Use {@link Builder} if you need to specify any of the optional parameters.
   *
   * @param transport HTTP transport, which should normally be:
   *     <ul>
   *       <li>Google App Engine: {@code
   *           com.google.api.client.extensions.appengine.http.UrlFetchTransport}
   *       <li>Android: {@code newCompatibleTransport} from {@code
   *           com.google.api.client.extensions.android.http.AndroidHttp}
   *       <li>Java: {@link
   *           com.google.api.client.googleapis.javanet.GoogleNetHttpTransport#newTrustedTransport()}
   *     </ul>
   *
   * @param jsonFactory JSON factory, which may be:
   *     <ul>
   *       <li>Jackson: {@code com.google.api.client.json.jackson2.JacksonFactory}
   *       <li>Google GSON: {@code com.google.api.client.json.gson.GsonFactory}
   *       <li>Android Honeycomb or higher: {@code
   *           com.google.api.client.extensions.android.json.AndroidJsonFactory}
   *     </ul>
   *
   * @param httpRequestInitializer HTTP request initializer or {@code null} for none
   * @since 1.7
   */
  public CloudDataplex(
      com.google.api.client.http.HttpTransport transport,
      com.google.api.client.json.JsonFactory jsonFactory,
      com.google.api.client.http.HttpRequestInitializer httpRequestInitializer) {
    this(new Builder(transport, jsonFactory, httpRequestInitializer));
  }

  /**
   * @param builder builder
   */
  CloudDataplex(Builder builder) {
    super(builder);
  }

  @Override
  protected void initialize(
      com.google.api.client.googleapis.services.AbstractGoogleClientRequest<?> httpClientRequest)
      throws java.io.IOException {
    super.initialize(httpClientRequest);
  }

  /**
   * An accessor for creating requests from the Projects collection.
   *
   * <p>The typical use is:
   *
   * <pre>
   *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
   *   {@code CloudDataplex.Projects.List request = dataplex.projects().list(parameters ...)}
   * </pre>
   *
   * @return the resource collection
   */
  public Projects projects() {
    return new Projects();
  }

  /** The "projects" collection of methods. */
  public class Projects {

    /**
     * An accessor for creating requests from the Locations collection.
     *
     * <p>The typical use is:
     *
     * <pre>
     *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
     *   {@code CloudDataplex.Locations.List request = dataplex.locations().list(parameters ...)}
     * </pre>
     *
     * @return the resource collection
     */
    public Locations locations() {
      return new Locations();
    }

    /** The "locations" collection of methods. */
    public class Locations {

      /**
       * Gets information about a location.
       *
       * <p>Create a request for the method "locations.get".
       *
       * <p>This request holds the parameters needed by the dataplex server. After setting any
       * optional parameters, call the {@link Get#execute()} method to invoke the remote operation.
       *
       * @param name Resource name for the location.
       * @return the request
       */
      public Get get(java.lang.String name) throws java.io.IOException {
        Get result = new Get(name);
        initialize(result);
        return result;
      }

      public class Get
          extends CloudDataplexRequest<
              com.google.api.services.dataplex.v1.model.GoogleCloudLocationLocation> {

        private static final String REST_PATH = "v1/{+name}";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

        /**
         * Gets information about a location.
         *
         * <p>Create a request for the method "locations.get".
         *
         * <p>This request holds the parameters needed by the the dataplex server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * <p>{@link
         * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
         * must be called to initialize this instance immediately after invoking the constructor.
         *
         * @param name Resource name for the location.
         * @since 1.13
         */
        protected Get(java.lang.String name) {
          super(
              CloudDataplex.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.dataplex.v1.model.GoogleCloudLocationLocation.class);
          this.name =
              com.google.api.client.util.Preconditions.checkNotNull(
                  name, "Required parameter name must be specified.");
          if (!getSuppressPatternChecks()) {
            com.google.api.client.util.Preconditions.checkArgument(
                NAME_PATTERN.matcher(name).matches(),
                "Parameter name must conform to the pattern " + "^projects/[^/]+/locations/[^/]+$");
          }
        }

        @Override
        public com.google.api.client.http.HttpResponse executeUsingHead()
            throws java.io.IOException {
          return super.executeUsingHead();
        }

        @Override
        public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
            throws java.io.IOException {
          return super.buildHttpRequestUsingHead();
        }

        @Override
        public Get set$Xgafv(java.lang.String $Xgafv) {
          return (Get) super.set$Xgafv($Xgafv);
        }

        @Override
        public Get setAccessToken(java.lang.String accessToken) {
          return (Get) super.setAccessToken(accessToken);
        }

        @Override
        public Get setAlt(java.lang.String alt) {
          return (Get) super.setAlt(alt);
        }

        @Override
        public Get setCallback(java.lang.String callback) {
          return (Get) super.setCallback(callback);
        }

        @Override
        public Get setFields(java.lang.String fields) {
          return (Get) super.setFields(fields);
        }

        @Override
        public Get setKey(java.lang.String key) {
          return (Get) super.setKey(key);
        }

        @Override
        public Get setOauthToken(java.lang.String oauthToken) {
          return (Get) super.setOauthToken(oauthToken);
        }

        @Override
        public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
          return (Get) super.setPrettyPrint(prettyPrint);
        }

        @Override
        public Get setQuotaUser(java.lang.String quotaUser) {
          return (Get) super.setQuotaUser(quotaUser);
        }

        @Override
        public Get setUploadType(java.lang.String uploadType) {
          return (Get) super.setUploadType(uploadType);
        }

        @Override
        public Get setUploadProtocol(java.lang.String uploadProtocol) {
          return (Get) super.setUploadProtocol(uploadProtocol);
        }

        /** Resource name for the location. */
        @com.google.api.client.util.Key private java.lang.String name;

        /** Resource name for the location. */
        public java.lang.String getName() {
          return name;
        }

        /** Resource name for the location. */
        public Get setName(java.lang.String name) {
          if (!getSuppressPatternChecks()) {
            com.google.api.client.util.Preconditions.checkArgument(
                NAME_PATTERN.matcher(name).matches(),
                "Parameter name must conform to the pattern " + "^projects/[^/]+/locations/[^/]+$");
          }
          this.name = name;
          return this;
        }

        @Override
        public Get set(String parameterName, Object value) {
          return (Get) super.set(parameterName, value);
        }
      }
      /**
       * Lists information about the supported locations for this service.
       *
       * <p>Create a request for the method "locations.list".
       *
       * <p>This request holds the parameters needed by the dataplex server. After setting any
       * optional parameters, call the {@link List#execute()} method to invoke the remote operation.
       *
       * @param name The resource that owns the locations collection, if applicable.
       * @return the request
       */
      public List list(java.lang.String name) throws java.io.IOException {
        List result = new List(name);
        initialize(result);
        return result;
      }

      public class List
          extends CloudDataplexRequest<
              com.google.api.services.dataplex.v1.model.GoogleCloudLocationListLocationsResponse> {

        private static final String REST_PATH = "v1/{+name}/locations";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+$");

        /**
         * Lists information about the supported locations for this service.
         *
         * <p>Create a request for the method "locations.list".
         *
         * <p>This request holds the parameters needed by the the dataplex server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * <p>{@link
         * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
         * must be called to initialize this instance immediately after invoking the constructor.
         *
         * @param name The resource that owns the locations collection, if applicable.
         * @since 1.13
         */
        protected List(java.lang.String name) {
          super(
              CloudDataplex.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.dataplex.v1.model.GoogleCloudLocationListLocationsResponse
                  .class);
          this.name =
              com.google.api.client.util.Preconditions.checkNotNull(
                  name, "Required parameter name must be specified.");
          if (!getSuppressPatternChecks()) {
            com.google.api.client.util.Preconditions.checkArgument(
                NAME_PATTERN.matcher(name).matches(),
                "Parameter name must conform to the pattern " + "^projects/[^/]+$");
          }
        }

        @Override
        public com.google.api.client.http.HttpResponse executeUsingHead()
            throws java.io.IOException {
          return super.executeUsingHead();
        }

        @Override
        public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
            throws java.io.IOException {
          return super.buildHttpRequestUsingHead();
        }

        @Override
        public List set$Xgafv(java.lang.String $Xgafv) {
          return (List) super.set$Xgafv($Xgafv);
        }

        @Override
        public List setAccessToken(java.lang.String accessToken) {
          return (List) super.setAccessToken(accessToken);
        }

        @Override
        public List setAlt(java.lang.String alt) {
          return (List) super.setAlt(alt);
        }

        @Override
        public List setCallback(java.lang.String callback) {
          return (List) super.setCallback(callback);
        }

        @Override
        public List setFields(java.lang.String fields) {
          return (List) super.setFields(fields);
        }

        @Override
        public List setKey(java.lang.String key) {
          return (List) super.setKey(key);
        }

        @Override
        public List setOauthToken(java.lang.String oauthToken) {
          return (List) super.setOauthToken(oauthToken);
        }

        @Override
        public List setPrettyPrint(java.lang.Boolean prettyPrint) {
          return (List) super.setPrettyPrint(prettyPrint);
        }

        @Override
        public List setQuotaUser(java.lang.String quotaUser) {
          return (List) super.setQuotaUser(quotaUser);
        }

        @Override
        public List setUploadType(java.lang.String uploadType) {
          return (List) super.setUploadType(uploadType);
        }

        @Override
        public List setUploadProtocol(java.lang.String uploadProtocol) {
          return (List) super.setUploadProtocol(uploadProtocol);
        }

        /** The resource that owns the locations collection, if applicable. */
        @com.google.api.client.util.Key private java.lang.String name;

        /** The resource that owns the locations collection, if applicable. */
        public java.lang.String getName() {
          return name;
        }

        /** The resource that owns the locations collection, if applicable. */
        public List setName(java.lang.String name) {
          if (!getSuppressPatternChecks()) {
            com.google.api.client.util.Preconditions.checkArgument(
                NAME_PATTERN.matcher(name).matches(),
                "Parameter name must conform to the pattern " + "^projects/[^/]+$");
          }
          this.name = name;
          return this;
        }

        /**
         * A filter to narrow down results to a preferred subset. The filtering language accepts
         * strings like "displayName=tokyo", and is documented in more detail in AIP-160
         * (https://google.aip.dev/160).
         */
        @com.google.api.client.util.Key private java.lang.String filter;

        /**
         * A filter to narrow down results to a preferred subset. The filtering language accepts
         * strings like "displayName=tokyo", and is documented in more detail in AIP-160
         * (https://google.aip.dev/160).
         */
        public java.lang.String getFilter() {
          return filter;
        }

        /**
         * A filter to narrow down results to a preferred subset. The filtering language accepts
         * strings like "displayName=tokyo", and is documented in more detail in AIP-160
         * (https://google.aip.dev/160).
         */
        public List setFilter(java.lang.String filter) {
          this.filter = filter;
          return this;
        }

        /** If true, the returned list will include locations which are not yet revealed. */
        @com.google.api.client.util.Key private java.lang.Boolean includeUnrevealedLocations;

        /** If true, the returned list will include locations which are not yet revealed. */
        public java.lang.Boolean getIncludeUnrevealedLocations() {
          return includeUnrevealedLocations;
        }

        /** If true, the returned list will include locations which are not yet revealed. */
        public List setIncludeUnrevealedLocations(java.lang.Boolean includeUnrevealedLocations) {
          this.includeUnrevealedLocations = includeUnrevealedLocations;
          return this;
        }

        /** The maximum number of results to return. If not set, the service selects a default. */
        @com.google.api.client.util.Key private java.lang.Integer pageSize;

        /** The maximum number of results to return. If not set, the service selects a default. */
        public java.lang.Integer getPageSize() {
          return pageSize;
        }

        /** The maximum number of results to return. If not set, the service selects a default. */
        public List setPageSize(java.lang.Integer pageSize) {
          this.pageSize = pageSize;
          return this;
        }

        /**
         * A page token received from the next_page_token field in the response. Send that page
         * token to receive the subsequent page.
         */
        @com.google.api.client.util.Key private java.lang.String pageToken;

        /**
         * A page token received from the next_page_token field in the response. Send that page
         * token to receive the subsequent page.
         */
        public java.lang.String getPageToken() {
          return pageToken;
        }

        /**
         * A page token received from the next_page_token field in the response. Send that page
         * token to receive the subsequent page.
         */
        public List setPageToken(java.lang.String pageToken) {
          this.pageToken = pageToken;
          return this;
        }

        @Override
        public List set(String parameterName, Object value) {
          return (List) super.set(parameterName, value);
        }
      }

      /**
       * An accessor for creating requests from the Lakes collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
       *   {@code CloudDataplex.Lakes.List request = dataplex.lakes().list(parameters ...)}
       * </pre>
       *
       * @return the resource collection
       */
      public Lakes lakes() {
        return new Lakes();
      }

      /** The "lakes" collection of methods. */
      public class Lakes {

        /**
         * Creates a lake resource.
         *
         * <p>Create a request for the method "lakes.create".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Create#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The resource name of the lake location, of the form:
         *     projects/{project_number}/locations/{location_id} where location_id refers to a GCP
         *     region.
         * @param content the {@link
         *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake}
         * @return the request
         */
        public Create create(
            java.lang.String parent,
            com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

          private static final String REST_PATH = "v1/{+parent}/lakes";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Creates a lake resource.
           *
           * <p>Create a request for the method "lakes.create".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The resource name of the lake location, of the form:
           *     projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           *     region.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake content) {
            super(
                CloudDataplex.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
            this.parent =
                com.google.api.client.util.Preconditions.checkNotNull(
                    parent, "Required parameter parent must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  PARENT_PATTERN.matcher(parent).matches(),
                  "Parameter parent must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
          }

          @Override
          public Create set$Xgafv(java.lang.String $Xgafv) {
            return (Create) super.set$Xgafv($Xgafv);
          }

          @Override
          public Create setAccessToken(java.lang.String accessToken) {
            return (Create) super.setAccessToken(accessToken);
          }

          @Override
          public Create setAlt(java.lang.String alt) {
            return (Create) super.setAlt(alt);
          }

          @Override
          public Create setCallback(java.lang.String callback) {
            return (Create) super.setCallback(callback);
          }

          @Override
          public Create setFields(java.lang.String fields) {
            return (Create) super.setFields(fields);
          }

          @Override
          public Create setKey(java.lang.String key) {
            return (Create) super.setKey(key);
          }

          @Override
          public Create setOauthToken(java.lang.String oauthToken) {
            return (Create) super.setOauthToken(oauthToken);
          }

          @Override
          public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Create) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Create setQuotaUser(java.lang.String quotaUser) {
            return (Create) super.setQuotaUser(quotaUser);
          }

          @Override
          public Create setUploadType(java.lang.String uploadType) {
            return (Create) super.setUploadType(uploadType);
          }

          @Override
          public Create setUploadProtocol(java.lang.String uploadProtocol) {
            return (Create) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Required. The resource name of the lake location, of the form:
           * projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           * region.
           */
          @com.google.api.client.util.Key private java.lang.String parent;

          /**
           * Required. The resource name of the lake location, of the form:
           * projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           * region.
           */
          public java.lang.String getParent() {
            return parent;
          }

          /**
           * Required. The resource name of the lake location, of the form:
           * projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           * region.
           */
          public Create setParent(java.lang.String parent) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  PARENT_PATTERN.matcher(parent).matches(),
                  "Parameter parent must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
            this.parent = parent;
            return this;
          }

          /**
           * Required. Lake identifier. This ID will be used to generate names such as database and
           * dataset names when publishing metadata to Hive Metastore and BigQuery. * Must contain
           * only lowercase letters, numbers and hyphens. * Must start with a letter. * Must end
           * with a number or a letter. * Must be between 1-63 characters. * Must be unique within
           * the customer project / location.
           */
          @com.google.api.client.util.Key private java.lang.String lakeId;

          /**
           * Required. Lake identifier. This ID will be used to generate names such as database and
           * dataset names when publishing metadata to Hive Metastore and BigQuery. * Must contain
           * only lowercase letters, numbers and hyphens. * Must start with a letter. * Must end
           * with a number or a letter. * Must be between 1-63 characters. * Must be unique within
           * the customer project / location.
           */
          public java.lang.String getLakeId() {
            return lakeId;
          }

          /**
           * Required. Lake identifier. This ID will be used to generate names such as database and
           * dataset names when publishing metadata to Hive Metastore and BigQuery. * Must contain
           * only lowercase letters, numbers and hyphens. * Must start with a letter. * Must end
           * with a number or a letter. * Must be between 1-63 characters. * Must be unique within
           * the customer project / location.
           */
          public Create setLakeId(java.lang.String lakeId) {
            this.lakeId = lakeId;
            return this;
          }

          /**
           * Optional. Only validate the request, but do not perform mutations. The default is
           * false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the request, but do not perform mutations. The default is
           * false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the request, but do not perform mutations. The default is
           * false.
           */
          public Create setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          @Override
          public Create set(String parameterName, Object value) {
            return (Create) super.set(parameterName, value);
          }
        }
        /**
         * Deletes a lake resource. All zones within the lake must be deleted before the lake can be
         * deleted.
         *
         * <p>Create a request for the method "lakes.delete".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The resource name of the lake:
         *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Deletes a lake resource. All zones within the lake must be deleted before the lake can
           * be deleted.
           *
           * <p>Create a request for the method "lakes.delete".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The resource name of the lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                CloudDataplex.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public Delete set$Xgafv(java.lang.String $Xgafv) {
            return (Delete) super.set$Xgafv($Xgafv);
          }

          @Override
          public Delete setAccessToken(java.lang.String accessToken) {
            return (Delete) super.setAccessToken(accessToken);
          }

          @Override
          public Delete setAlt(java.lang.String alt) {
            return (Delete) super.setAlt(alt);
          }

          @Override
          public Delete setCallback(java.lang.String callback) {
            return (Delete) super.setCallback(callback);
          }

          @Override
          public Delete setFields(java.lang.String fields) {
            return (Delete) super.setFields(fields);
          }

          @Override
          public Delete setKey(java.lang.String key) {
            return (Delete) super.setKey(key);
          }

          @Override
          public Delete setOauthToken(java.lang.String oauthToken) {
            return (Delete) super.setOauthToken(oauthToken);
          }

          @Override
          public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Delete) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Delete setQuotaUser(java.lang.String quotaUser) {
            return (Delete) super.setQuotaUser(quotaUser);
          }

          @Override
          public Delete setUploadType(java.lang.String uploadType) {
            return (Delete) super.setUploadType(uploadType);
          }

          @Override
          public Delete setUploadProtocol(java.lang.String uploadProtocol) {
            return (Delete) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Required. The resource name of the lake:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          @com.google.api.client.util.Key private java.lang.String name;

          /**
           * Required. The resource name of the lake:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public java.lang.String getName() {
            return name;
          }

          /**
           * Required. The resource name of the lake:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public Delete setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Delete set(String parameterName, Object value) {
            return (Delete) super.set(parameterName, value);
          }
        }
        /**
         * Retrieves a lake resource.
         *
         * <p>Create a request for the method "lakes.get".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The resource name of the lake:
         *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Retrieves a lake resource.
           *
           * <p>Create a request for the method "lakes.get".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The resource name of the lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                CloudDataplex.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public com.google.api.client.http.HttpResponse executeUsingHead()
              throws java.io.IOException {
            return super.executeUsingHead();
          }

          @Override
          public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
              throws java.io.IOException {
            return super.buildHttpRequestUsingHead();
          }

          @Override
          public Get set$Xgafv(java.lang.String $Xgafv) {
            return (Get) super.set$Xgafv($Xgafv);
          }

          @Override
          public Get setAccessToken(java.lang.String accessToken) {
            return (Get) super.setAccessToken(accessToken);
          }

          @Override
          public Get setAlt(java.lang.String alt) {
            return (Get) super.setAlt(alt);
          }

          @Override
          public Get setCallback(java.lang.String callback) {
            return (Get) super.setCallback(callback);
          }

          @Override
          public Get setFields(java.lang.String fields) {
            return (Get) super.setFields(fields);
          }

          @Override
          public Get setKey(java.lang.String key) {
            return (Get) super.setKey(key);
          }

          @Override
          public Get setOauthToken(java.lang.String oauthToken) {
            return (Get) super.setOauthToken(oauthToken);
          }

          @Override
          public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Get) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Get setQuotaUser(java.lang.String quotaUser) {
            return (Get) super.setQuotaUser(quotaUser);
          }

          @Override
          public Get setUploadType(java.lang.String uploadType) {
            return (Get) super.setUploadType(uploadType);
          }

          @Override
          public Get setUploadProtocol(java.lang.String uploadProtocol) {
            return (Get) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Required. The resource name of the lake:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          @com.google.api.client.util.Key private java.lang.String name;

          /**
           * Required. The resource name of the lake:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public java.lang.String getName() {
            return name;
          }

          /**
           * Required. The resource name of the lake:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public Get setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Get set(String parameterName, Object value) {
            return (Get) super.set(parameterName, value);
          }
        }
        /**
         * Gets the access control policy for a resource. Returns an empty policy if the resource
         * exists and does not have a policy set.
         *
         * <p>Create a request for the method "lakes.getIamPolicy".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the remote
         * operation.
         *
         * @param resource REQUIRED: The resource for which the policy is being requested. See the
         *     operation documentation for the appropriate value for this field.
         * @return the request
         */
        public GetIamPolicy getIamPolicy(java.lang.String resource) throws java.io.IOException {
          GetIamPolicy result = new GetIamPolicy(resource);
          initialize(result);
          return result;
        }

        public class GetIamPolicy
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

          private static final String REST_PATH = "v1/{+resource}:getIamPolicy";

          private final java.util.regex.Pattern RESOURCE_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Gets the access control policy for a resource. Returns an empty policy if the resource
           * exists and does not have a policy set.
           *
           * <p>Create a request for the method "lakes.getIamPolicy".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * <p>{@link
           * GetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param resource REQUIRED: The resource for which the policy is being requested. See the
           *     operation documentation for the appropriate value for this field.
           * @since 1.13
           */
          protected GetIamPolicy(java.lang.String resource) {
            super(
                CloudDataplex.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
            this.resource =
                com.google.api.client.util.Preconditions.checkNotNull(
                    resource, "Required parameter resource must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  RESOURCE_PATTERN.matcher(resource).matches(),
                  "Parameter resource must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public com.google.api.client.http.HttpResponse executeUsingHead()
              throws java.io.IOException {
            return super.executeUsingHead();
          }

          @Override
          public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
              throws java.io.IOException {
            return super.buildHttpRequestUsingHead();
          }

          @Override
          public GetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
            return (GetIamPolicy) super.set$Xgafv($Xgafv);
          }

          @Override
          public GetIamPolicy setAccessToken(java.lang.String accessToken) {
            return (GetIamPolicy) super.setAccessToken(accessToken);
          }

          @Override
          public GetIamPolicy setAlt(java.lang.String alt) {
            return (GetIamPolicy) super.setAlt(alt);
          }

          @Override
          public GetIamPolicy setCallback(java.lang.String callback) {
            return (GetIamPolicy) super.setCallback(callback);
          }

          @Override
          public GetIamPolicy setFields(java.lang.String fields) {
            return (GetIamPolicy) super.setFields(fields);
          }

          @Override
          public GetIamPolicy setKey(java.lang.String key) {
            return (GetIamPolicy) super.setKey(key);
          }

          @Override
          public GetIamPolicy setOauthToken(java.lang.String oauthToken) {
            return (GetIamPolicy) super.setOauthToken(oauthToken);
          }

          @Override
          public GetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (GetIamPolicy) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public GetIamPolicy setQuotaUser(java.lang.String quotaUser) {
            return (GetIamPolicy) super.setQuotaUser(quotaUser);
          }

          @Override
          public GetIamPolicy setUploadType(java.lang.String uploadType) {
            return (GetIamPolicy) super.setUploadType(uploadType);
          }

          @Override
          public GetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
            return (GetIamPolicy) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * REQUIRED: The resource for which the policy is being requested. See the operation
           * documentation for the appropriate value for this field.
           */
          @com.google.api.client.util.Key private java.lang.String resource;

          /**
           * REQUIRED: The resource for which the policy is being requested. See the operation
           * documentation for the appropriate value for this field.
           */
          public java.lang.String getResource() {
            return resource;
          }

          /**
           * REQUIRED: The resource for which the policy is being requested. See the operation
           * documentation for the appropriate value for this field.
           */
          public GetIamPolicy setResource(java.lang.String resource) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  RESOURCE_PATTERN.matcher(resource).matches(),
                  "Parameter resource must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.resource = resource;
            return this;
          }

          /**
           * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
           * Requests specifying an invalid value will be rejected.Requests for policies with any
           * conditional bindings must specify version 3. Policies without any conditional bindings
           * may specify any valid value or leave the field unset.To learn which resources support
           * conditions in their IAM policies, see the IAM documentation
           * (https://cloud.google.com/iam/help/conditions/resource-policies).
           */
          @com.google.api.client.util.Key("options.requestedPolicyVersion")
          private java.lang.Integer optionsRequestedPolicyVersion;

          /**
           * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
           * Requests specifying an invalid value will be rejected.Requests for policies with any
           * conditional bindings must specify version 3. Policies without any conditional bindings
           * may specify any valid value or leave the field unset.To learn which resources support
           * conditions in their IAM policies, see the IAM documentation
           * (https://cloud.google.com/iam/help/conditions/resource-policies).
           */
          public java.lang.Integer getOptionsRequestedPolicyVersion() {
            return optionsRequestedPolicyVersion;
          }

          /**
           * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
           * Requests specifying an invalid value will be rejected.Requests for policies with any
           * conditional bindings must specify version 3. Policies without any conditional bindings
           * may specify any valid value or leave the field unset.To learn which resources support
           * conditions in their IAM policies, see the IAM documentation
           * (https://cloud.google.com/iam/help/conditions/resource-policies).
           */
          public GetIamPolicy setOptionsRequestedPolicyVersion(
              java.lang.Integer optionsRequestedPolicyVersion) {
            this.optionsRequestedPolicyVersion = optionsRequestedPolicyVersion;
            return this;
          }

          @Override
          public GetIamPolicy set(String parameterName, Object value) {
            return (GetIamPolicy) super.set(parameterName, value);
          }
        }
        /**
         * Lists lake resources in a project and location.
         *
         * <p>Create a request for the method "lakes.list".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The resource name of the lake location, of the form:
         *     projects/{project_number}/locations/{location_id} where location_id refers to a GCP
         *     region.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListLakesResponse> {

          private static final String REST_PATH = "v1/{+parent}/lakes";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Lists lake resources in a project and location.
           *
           * <p>Create a request for the method "lakes.list".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The resource name of the lake location, of the form:
           *     projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           *     region.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                CloudDataplex.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListLakesResponse
                    .class);
            this.parent =
                com.google.api.client.util.Preconditions.checkNotNull(
                    parent, "Required parameter parent must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  PARENT_PATTERN.matcher(parent).matches(),
                  "Parameter parent must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
          }

          @Override
          public com.google.api.client.http.HttpResponse executeUsingHead()
              throws java.io.IOException {
            return super.executeUsingHead();
          }

          @Override
          public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
              throws java.io.IOException {
            return super.buildHttpRequestUsingHead();
          }

          @Override
          public List set$Xgafv(java.lang.String $Xgafv) {
            return (List) super.set$Xgafv($Xgafv);
          }

          @Override
          public List setAccessToken(java.lang.String accessToken) {
            return (List) super.setAccessToken(accessToken);
          }

          @Override
          public List setAlt(java.lang.String alt) {
            return (List) super.setAlt(alt);
          }

          @Override
          public List setCallback(java.lang.String callback) {
            return (List) super.setCallback(callback);
          }

          @Override
          public List setFields(java.lang.String fields) {
            return (List) super.setFields(fields);
          }

          @Override
          public List setKey(java.lang.String key) {
            return (List) super.setKey(key);
          }

          @Override
          public List setOauthToken(java.lang.String oauthToken) {
            return (List) super.setOauthToken(oauthToken);
          }

          @Override
          public List setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (List) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public List setQuotaUser(java.lang.String quotaUser) {
            return (List) super.setQuotaUser(quotaUser);
          }

          @Override
          public List setUploadType(java.lang.String uploadType) {
            return (List) super.setUploadType(uploadType);
          }

          @Override
          public List setUploadProtocol(java.lang.String uploadProtocol) {
            return (List) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Required. The resource name of the lake location, of the form:
           * projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           * region.
           */
          @com.google.api.client.util.Key private java.lang.String parent;

          /**
           * Required. The resource name of the lake location, of the form:
           * projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           * region.
           */
          public java.lang.String getParent() {
            return parent;
          }

          /**
           * Required. The resource name of the lake location, of the form:
           * projects/{project_number}/locations/{location_id} where location_id refers to a GCP
           * region.
           */
          public List setParent(java.lang.String parent) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  PARENT_PATTERN.matcher(parent).matches(),
                  "Parameter parent must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
            this.parent = parent;
            return this;
          }

          /** Optional. Filter request. */
          @com.google.api.client.util.Key private java.lang.String filter;

          /** Optional. Filter request. */
          public java.lang.String getFilter() {
            return filter;
          }

          /** Optional. Filter request. */
          public List setFilter(java.lang.String filter) {
            this.filter = filter;
            return this;
          }

          /** Optional. Order by fields for the result. */
          @com.google.api.client.util.Key private java.lang.String orderBy;

          /** Optional. Order by fields for the result. */
          public java.lang.String getOrderBy() {
            return orderBy;
          }

          /** Optional. Order by fields for the result. */
          public List setOrderBy(java.lang.String orderBy) {
            this.orderBy = orderBy;
            return this;
          }

          /**
           * Optional. Maximum number of Lakes to return. The service may return fewer than this
           * value. If unspecified, at most 10 lakes will be returned. The maximum value is 1000;
           * values above 1000 will be coerced to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Optional. Maximum number of Lakes to return. The service may return fewer than this
           * value. If unspecified, at most 10 lakes will be returned. The maximum value is 1000;
           * values above 1000 will be coerced to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Optional. Maximum number of Lakes to return. The service may return fewer than this
           * value. If unspecified, at most 10 lakes will be returned. The maximum value is 1000;
           * values above 1000 will be coerced to 1000.
           */
          public List setPageSize(java.lang.Integer pageSize) {
            this.pageSize = pageSize;
            return this;
          }

          /**
           * Optional. Page token received from a previous ListLakes call. Provide this to retrieve
           * the subsequent page. When paginating, all other parameters provided to ListLakes must
           * match the call that provided the page token.
           */
          @com.google.api.client.util.Key private java.lang.String pageToken;

          /**
           * Optional. Page token received from a previous ListLakes call. Provide this to retrieve
           * the subsequent page. When paginating, all other parameters provided to ListLakes must
           * match the call that provided the page token.
           */
          public java.lang.String getPageToken() {
            return pageToken;
          }

          /**
           * Optional. Page token received from a previous ListLakes call. Provide this to retrieve
           * the subsequent page. When paginating, all other parameters provided to ListLakes must
           * match the call that provided the page token.
           */
          public List setPageToken(java.lang.String pageToken) {
            this.pageToken = pageToken;
            return this;
          }

          @Override
          public List set(String parameterName, Object value) {
            return (List) super.set(parameterName, value);
          }
        }
        /**
         * Updates a lake resource.
         *
         * <p>Create a request for the method "lakes.patch".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Patch#execute()} method to invoke the remote
         * operation.
         *
         * @param name Output only. The relative resource name of the lake, of the form:
         *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
         * @param content the {@link
         *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake}
         * @return the request
         */
        public Patch patch(
            java.lang.String name,
            com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake content)
            throws java.io.IOException {
          Patch result = new Patch(name, content);
          initialize(result);
          return result;
        }

        public class Patch
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Updates a lake resource.
           *
           * <p>Create a request for the method "lakes.patch".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Output only. The relative resource name of the lake, of the form:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake}
           * @since 1.13
           */
          protected Patch(
              java.lang.String name,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Lake content) {
            super(
                CloudDataplex.this,
                "PATCH",
                REST_PATH,
                content,
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public Patch set$Xgafv(java.lang.String $Xgafv) {
            return (Patch) super.set$Xgafv($Xgafv);
          }

          @Override
          public Patch setAccessToken(java.lang.String accessToken) {
            return (Patch) super.setAccessToken(accessToken);
          }

          @Override
          public Patch setAlt(java.lang.String alt) {
            return (Patch) super.setAlt(alt);
          }

          @Override
          public Patch setCallback(java.lang.String callback) {
            return (Patch) super.setCallback(callback);
          }

          @Override
          public Patch setFields(java.lang.String fields) {
            return (Patch) super.setFields(fields);
          }

          @Override
          public Patch setKey(java.lang.String key) {
            return (Patch) super.setKey(key);
          }

          @Override
          public Patch setOauthToken(java.lang.String oauthToken) {
            return (Patch) super.setOauthToken(oauthToken);
          }

          @Override
          public Patch setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Patch) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Patch setQuotaUser(java.lang.String quotaUser) {
            return (Patch) super.setQuotaUser(quotaUser);
          }

          @Override
          public Patch setUploadType(java.lang.String uploadType) {
            return (Patch) super.setUploadType(uploadType);
          }

          @Override
          public Patch setUploadProtocol(java.lang.String uploadProtocol) {
            return (Patch) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Output only. The relative resource name of the lake, of the form:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          @com.google.api.client.util.Key private java.lang.String name;

          /**
           * Output only. The relative resource name of the lake, of the form:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public java.lang.String getName() {
            return name;
          }

          /**
           * Output only. The relative resource name of the lake, of the form:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public Patch setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /** Required. Mask of fields to update. */
          @com.google.api.client.util.Key private String updateMask;

          /** Required. Mask of fields to update. */
          public String getUpdateMask() {
            return updateMask;
          }

          /** Required. Mask of fields to update. */
          public Patch setUpdateMask(String updateMask) {
            this.updateMask = updateMask;
            return this;
          }

          /**
           * Optional. Only validate the request, but do not perform mutations. The default is
           * false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the request, but do not perform mutations. The default is
           * false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the request, but do not perform mutations. The default is
           * false.
           */
          public Patch setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          @Override
          public Patch set(String parameterName, Object value) {
            return (Patch) super.set(parameterName, value);
          }
        }
        /**
         * Marks actions associated with a lake as resolved.
         *
         * <p>Create a request for the method "lakes.resolveLakeActions".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link ResolveLakeActions#execute()} method to invoke the
         * remote operation.
         *
         * @param lake Required. The name of the lake for which actions are being resolved of the
         *     form: projects/{project_number}/locations/{location_id}/lakes/{lake_id}
         * @param content the {@link
         *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveLakeActionsRequest}
         * @return the request
         */
        public ResolveLakeActions resolveLakeActions(
            java.lang.String lake,
            com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveLakeActionsRequest
                content)
            throws java.io.IOException {
          ResolveLakeActions result = new ResolveLakeActions(lake, content);
          initialize(result);
          return result;
        }

        public class ResolveLakeActions
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

          private static final String REST_PATH = "v1/{+lake}:resolveLakeActions";

          private final java.util.regex.Pattern LAKE_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Marks actions associated with a lake as resolved.
           *
           * <p>Create a request for the method "lakes.resolveLakeActions".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link ResolveLakeActions#execute()} method to invoke
           * the remote operation.
           *
           * <p>{@link ResolveLakeActions#initialize(com.google.api.client.googleapis.services.A
           * bstractGoogleClientRequest)} must be called to initialize this instance immediately
           * after invoking the constructor.
           *
           * @param lake Required. The name of the lake for which actions are being resolved of the
           *     form: projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveLakeActionsRequest}
           * @since 1.13
           */
          protected ResolveLakeActions(
              java.lang.String lake,
              com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ResolveLakeActionsRequest
                  content) {
            super(
                CloudDataplex.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
            this.lake =
                com.google.api.client.util.Preconditions.checkNotNull(
                    lake, "Required parameter lake must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  LAKE_PATTERN.matcher(lake).matches(),
                  "Parameter lake must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public ResolveLakeActions set$Xgafv(java.lang.String $Xgafv) {
            return (ResolveLakeActions) super.set$Xgafv($Xgafv);
          }

          @Override
          public ResolveLakeActions setAccessToken(java.lang.String accessToken) {
            return (ResolveLakeActions) super.setAccessToken(accessToken);
          }

          @Override
          public ResolveLakeActions setAlt(java.lang.String alt) {
            return (ResolveLakeActions) super.setAlt(alt);
          }

          @Override
          public ResolveLakeActions setCallback(java.lang.String callback) {
            return (ResolveLakeActions) super.setCallback(callback);
          }

          @Override
          public ResolveLakeActions setFields(java.lang.String fields) {
            return (ResolveLakeActions) super.setFields(fields);
          }

          @Override
          public ResolveLakeActions setKey(java.lang.String key) {
            return (ResolveLakeActions) super.setKey(key);
          }

          @Override
          public ResolveLakeActions setOauthToken(java.lang.String oauthToken) {
            return (ResolveLakeActions) super.setOauthToken(oauthToken);
          }

          @Override
          public ResolveLakeActions setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (ResolveLakeActions) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public ResolveLakeActions setQuotaUser(java.lang.String quotaUser) {
            return (ResolveLakeActions) super.setQuotaUser(quotaUser);
          }

          @Override
          public ResolveLakeActions setUploadType(java.lang.String uploadType) {
            return (ResolveLakeActions) super.setUploadType(uploadType);
          }

          @Override
          public ResolveLakeActions setUploadProtocol(java.lang.String uploadProtocol) {
            return (ResolveLakeActions) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Required. The name of the lake for which actions are being resolved of the form:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          @com.google.api.client.util.Key private java.lang.String lake;

          /**
           * Required. The name of the lake for which actions are being resolved of the form:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public java.lang.String getLake() {
            return lake;
          }

          /**
           * Required. The name of the lake for which actions are being resolved of the form:
           * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           */
          public ResolveLakeActions setLake(java.lang.String lake) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  LAKE_PATTERN.matcher(lake).matches(),
                  "Parameter lake must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.lake = lake;
            return this;
          }

          @Override
          public ResolveLakeActions set(String parameterName, Object value) {
            return (ResolveLakeActions) super.set(parameterName, value);
          }
        }
        /**
         * Sets the access control policy on the specified resource. Replaces any existing
         * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
         *
         * <p>Create a request for the method "lakes.setIamPolicy".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the remote
         * operation.
         *
         * @param resource REQUIRED: The resource for which the policy is being specified. See the
         *     operation documentation for the appropriate value for this field.
         * @param content the {@link
         *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
         * @return the request
         */
        public SetIamPolicy setIamPolicy(
            java.lang.String resource,
            com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content)
            throws java.io.IOException {
          SetIamPolicy result = new SetIamPolicy(resource, content);
          initialize(result);
          return result;
        }

        public class SetIamPolicy
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

          private static final String REST_PATH = "v1/{+resource}:setIamPolicy";

          private final java.util.regex.Pattern RESOURCE_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Sets the access control policy on the specified resource. Replaces any existing
           * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
           *
           * <p>Create a request for the method "lakes.setIamPolicy".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * <p>{@link
           * SetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param resource REQUIRED: The resource for which the policy is being specified. See the
           *     operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
           * @since 1.13
           */
          protected SetIamPolicy(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content) {
            super(
                CloudDataplex.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
            this.resource =
                com.google.api.client.util.Preconditions.checkNotNull(
                    resource, "Required parameter resource must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  RESOURCE_PATTERN.matcher(resource).matches(),
                  "Parameter resource must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public SetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
            return (SetIamPolicy) super.set$Xgafv($Xgafv);
          }

          @Override
          public SetIamPolicy setAccessToken(java.lang.String accessToken) {
            return (SetIamPolicy) super.setAccessToken(accessToken);
          }

          @Override
          public SetIamPolicy setAlt(java.lang.String alt) {
            return (SetIamPolicy) super.setAlt(alt);
          }

          @Override
          public SetIamPolicy setCallback(java.lang.String callback) {
            return (SetIamPolicy) super.setCallback(callback);
          }

          @Override
          public SetIamPolicy setFields(java.lang.String fields) {
            return (SetIamPolicy) super.setFields(fields);
          }

          @Override
          public SetIamPolicy setKey(java.lang.String key) {
            return (SetIamPolicy) super.setKey(key);
          }

          @Override
          public SetIamPolicy setOauthToken(java.lang.String oauthToken) {
            return (SetIamPolicy) super.setOauthToken(oauthToken);
          }

          @Override
          public SetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (SetIamPolicy) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public SetIamPolicy setQuotaUser(java.lang.String quotaUser) {
            return (SetIamPolicy) super.setQuotaUser(quotaUser);
          }

          @Override
          public SetIamPolicy setUploadType(java.lang.String uploadType) {
            return (SetIamPolicy) super.setUploadType(uploadType);
          }

          @Override
          public SetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
            return (SetIamPolicy) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * REQUIRED: The resource for which the policy is being specified. See the operation
           * documentation for the appropriate value for this field.
           */
          @com.google.api.client.util.Key private java.lang.String resource;

          /**
           * REQUIRED: The resource for which the policy is being specified. See the operation
           * documentation for the appropriate value for this field.
           */
          public java.lang.String getResource() {
            return resource;
          }

          /**
           * REQUIRED: The resource for which the policy is being specified. See the operation
           * documentation for the appropriate value for this field.
           */
          public SetIamPolicy setResource(java.lang.String resource) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  RESOURCE_PATTERN.matcher(resource).matches(),
                  "Parameter resource must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.resource = resource;
            return this;
          }

          @Override
          public SetIamPolicy set(String parameterName, Object value) {
            return (SetIamPolicy) super.set(parameterName, value);
          }
        }
        /**
         * Returns permissions that a caller has on the specified resource. If the resource does not
         * exist, this will return an empty set of permissions, not a NOT_FOUND error.Note: This
         * operation is designed to be used for building permission-aware UIs and command-line
         * tools, not for authorization checking. This operation may "fail open" without warning.
         *
         * <p>Create a request for the method "lakes.testIamPermissions".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link TestIamPermissions#execute()} method to invoke the
         * remote operation.
         *
         * @param resource REQUIRED: The resource for which the policy detail is being requested.
         *     See the operation documentation for the appropriate value for this field.
         * @param content the {@link
         *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
         * @return the request
         */
        public TestIamPermissions testIamPermissions(
            java.lang.String resource,
            com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest content)
            throws java.io.IOException {
          TestIamPermissions result = new TestIamPermissions(resource, content);
          initialize(result);
          return result;
        }

        public class TestIamPermissions
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse> {

          private static final String REST_PATH = "v1/{+resource}:testIamPermissions";

          private final java.util.regex.Pattern RESOURCE_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

          /**
           * Returns permissions that a caller has on the specified resource. If the resource does
           * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
           * This operation is designed to be used for building permission-aware UIs and
           * command-line tools, not for authorization checking. This operation may "fail open"
           * without warning.
           *
           * <p>Create a request for the method "lakes.testIamPermissions".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link TestIamPermissions#execute()} method to invoke
           * the remote operation.
           *
           * <p>{@link TestIamPermissions#initialize(com.google.api.client.googleapis.services.A
           * bstractGoogleClientRequest)} must be called to initialize this instance immediately
           * after invoking the constructor.
           *
           * @param resource REQUIRED: The resource for which the policy detail is being requested.
           *     See the operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
           * @since 1.13
           */
          protected TestIamPermissions(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                  content) {
            super(
                CloudDataplex.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse
                    .class);
            this.resource =
                com.google.api.client.util.Preconditions.checkNotNull(
                    resource, "Required parameter resource must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  RESOURCE_PATTERN.matcher(resource).matches(),
                  "Parameter resource must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
          }

          @Override
          public TestIamPermissions set$Xgafv(java.lang.String $Xgafv) {
            return (TestIamPermissions) super.set$Xgafv($Xgafv);
          }

          @Override
          public TestIamPermissions setAccessToken(java.lang.String accessToken) {
            return (TestIamPermissions) super.setAccessToken(accessToken);
          }

          @Override
          public TestIamPermissions setAlt(java.lang.String alt) {
            return (TestIamPermissions) super.setAlt(alt);
          }

          @Override
          public TestIamPermissions setCallback(java.lang.String callback) {
            return (TestIamPermissions) super.setCallback(callback);
          }

          @Override
          public TestIamPermissions setFields(java.lang.String fields) {
            return (TestIamPermissions) super.setFields(fields);
          }

          @Override
          public TestIamPermissions setKey(java.lang.String key) {
            return (TestIamPermissions) super.setKey(key);
          }

          @Override
          public TestIamPermissions setOauthToken(java.lang.String oauthToken) {
            return (TestIamPermissions) super.setOauthToken(oauthToken);
          }

          @Override
          public TestIamPermissions setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (TestIamPermissions) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public TestIamPermissions setQuotaUser(java.lang.String quotaUser) {
            return (TestIamPermissions) super.setQuotaUser(quotaUser);
          }

          @Override
          public TestIamPermissions setUploadType(java.lang.String uploadType) {
            return (TestIamPermissions) super.setUploadType(uploadType);
          }

          @Override
          public TestIamPermissions setUploadProtocol(java.lang.String uploadProtocol) {
            return (TestIamPermissions) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * REQUIRED: The resource for which the policy detail is being requested. See the
           * operation documentation for the appropriate value for this field.
           */
          @com.google.api.client.util.Key private java.lang.String resource;

          /**
           * REQUIRED: The resource for which the policy detail is being requested. See the
           * operation documentation for the appropriate value for this field.
           */
          public java.lang.String getResource() {
            return resource;
          }

          /**
           * REQUIRED: The resource for which the policy detail is being requested. See the
           * operation documentation for the appropriate value for this field.
           */
          public TestIamPermissions setResource(java.lang.String resource) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  RESOURCE_PATTERN.matcher(resource).matches(),
                  "Parameter resource must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
            }
            this.resource = resource;
            return this;
          }

          @Override
          public TestIamPermissions set(String parameterName, Object value) {
            return (TestIamPermissions) super.set(parameterName, value);
          }
        }

        /**
         * An accessor for creating requests from the Actions collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
         *   {@code CloudDataplex.Actions.List request = dataplex.actions().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Actions actions() {
          return new Actions();
        }

        /** The "actions" collection of methods. */
        public class Actions {

          /**
           * Lists action resources in a lake.
           *
           * <p>Create a request for the method "actions.list".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ListActionsResponse> {

            private static final String REST_PATH = "v1/{+parent}/actions";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Lists action resources in a lake.
             *
             * <p>Create a request for the method "actions.list".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListActionsResponse
                      .class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public List set$Xgafv(java.lang.String $Xgafv) {
              return (List) super.set$Xgafv($Xgafv);
            }

            @Override
            public List setAccessToken(java.lang.String accessToken) {
              return (List) super.setAccessToken(accessToken);
            }

            @Override
            public List setAlt(java.lang.String alt) {
              return (List) super.setAlt(alt);
            }

            @Override
            public List setCallback(java.lang.String callback) {
              return (List) super.setCallback(callback);
            }

            @Override
            public List setFields(java.lang.String fields) {
              return (List) super.setFields(fields);
            }

            @Override
            public List setKey(java.lang.String key) {
              return (List) super.setKey(key);
            }

            @Override
            public List setOauthToken(java.lang.String oauthToken) {
              return (List) super.setOauthToken(oauthToken);
            }

            @Override
            public List setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (List) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public List setQuotaUser(java.lang.String quotaUser) {
              return (List) super.setQuotaUser(quotaUser);
            }

            @Override
            public List setUploadType(java.lang.String uploadType) {
              return (List) super.setUploadType(uploadType);
            }

            @Override
            public List setUploadProtocol(java.lang.String uploadProtocol) {
              return (List) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Optional. Maximum number of actions to return. The service may return fewer than this
             * value. If unspecified, at most 10 actions will be returned. The maximum value is
             * 1000; values above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Optional. Maximum number of actions to return. The service may return fewer than this
             * value. If unspecified, at most 10 actions will be returned. The maximum value is
             * 1000; values above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Optional. Maximum number of actions to return. The service may return fewer than this
             * value. If unspecified, at most 10 actions will be returned. The maximum value is
             * 1000; values above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Optional. Page token received from a previous ListLakeActions call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListLakeActions must match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Optional. Page token received from a previous ListLakeActions call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListLakeActions must match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Optional. Page token received from a previous ListLakeActions call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListLakeActions must match the call that provided the page token.
             */
            public List setPageToken(java.lang.String pageToken) {
              this.pageToken = pageToken;
              return this;
            }

            @Override
            public List set(String parameterName, Object value) {
              return (List) super.set(parameterName, value);
            }
          }
        }
        /**
         * An accessor for creating requests from the Content collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
         *   {@code CloudDataplex.Content.List request = dataplex.content().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Content content() {
          return new Content();
        }

        /** The "content" collection of methods. */
        public class Content {

          /**
           * Create a content resource.
           *
           * <p>Create a request for the method "content.create".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content}
           * @return the request
           */
          public Create create(
              java.lang.String parent,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content content)
              throws java.io.IOException {
            Create result = new Create(parent, content);
            initialize(result);
            return result;
          }

          public class Create
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+parent}/content";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Create a content resource.
             *
             * <p>Create a request for the method "content.create".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Create#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content}
             * @since 1.13
             */
            protected Create(
                java.lang.String parent,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public Create set$Xgafv(java.lang.String $Xgafv) {
              return (Create) super.set$Xgafv($Xgafv);
            }

            @Override
            public Create setAccessToken(java.lang.String accessToken) {
              return (Create) super.setAccessToken(accessToken);
            }

            @Override
            public Create setAlt(java.lang.String alt) {
              return (Create) super.setAlt(alt);
            }

            @Override
            public Create setCallback(java.lang.String callback) {
              return (Create) super.setCallback(callback);
            }

            @Override
            public Create setFields(java.lang.String fields) {
              return (Create) super.setFields(fields);
            }

            @Override
            public Create setKey(java.lang.String key) {
              return (Create) super.setKey(key);
            }

            @Override
            public Create setOauthToken(java.lang.String oauthToken) {
              return (Create) super.setOauthToken(oauthToken);
            }

            @Override
            public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Create) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Create setQuotaUser(java.lang.String quotaUser) {
              return (Create) super.setQuotaUser(quotaUser);
            }

            @Override
            public Create setUploadType(java.lang.String uploadType) {
              return (Create) super.setUploadType(uploadType);
            }

            @Override
            public Create setUploadProtocol(java.lang.String uploadProtocol) {
              return (Create) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public Create setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Create setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Create set(String parameterName, Object value) {
              return (Create) super.set(parameterName, value);
            }
          }
          /**
           * Delete the content resource. All the child resources must have been deleted before
           * content deletion can be initiated.
           *
           * <p>Create a request for the method "content.delete".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the content:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
           * @return the request
           */
          public Delete delete(java.lang.String name) throws java.io.IOException {
            Delete result = new Delete(name);
            initialize(result);
            return result;
          }

          public class Delete
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");

            /**
             * Delete the content resource. All the child resources must have been deleted before
             * content deletion can be initiated.
             *
             * <p>Create a request for the method "content.delete".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Delete#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the content:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             * @since 1.13
             */
            protected Delete(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "DELETE",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
            }

            @Override
            public Delete set$Xgafv(java.lang.String $Xgafv) {
              return (Delete) super.set$Xgafv($Xgafv);
            }

            @Override
            public Delete setAccessToken(java.lang.String accessToken) {
              return (Delete) super.setAccessToken(accessToken);
            }

            @Override
            public Delete setAlt(java.lang.String alt) {
              return (Delete) super.setAlt(alt);
            }

            @Override
            public Delete setCallback(java.lang.String callback) {
              return (Delete) super.setCallback(callback);
            }

            @Override
            public Delete setFields(java.lang.String fields) {
              return (Delete) super.setFields(fields);
            }

            @Override
            public Delete setKey(java.lang.String key) {
              return (Delete) super.setKey(key);
            }

            @Override
            public Delete setOauthToken(java.lang.String oauthToken) {
              return (Delete) super.setOauthToken(oauthToken);
            }

            @Override
            public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Delete) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Delete setQuotaUser(java.lang.String quotaUser) {
              return (Delete) super.setQuotaUser(quotaUser);
            }

            @Override
            public Delete setUploadType(java.lang.String uploadType) {
              return (Delete) super.setUploadType(uploadType);
            }

            @Override
            public Delete setUploadProtocol(java.lang.String uploadProtocol) {
              return (Delete) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the content:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the content:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the content:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            public Delete setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Delete set(String parameterName, Object value) {
              return (Delete) super.set(parameterName, value);
            }
          }
          /**
           * Get content resource.
           *
           * <p>Create a request for the method "content.get".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the content:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
           * @return the request
           */
          public Get get(java.lang.String name) throws java.io.IOException {
            Get result = new Get(name);
            initialize(result);
            return result;
          }

          public class Get
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");

            /**
             * Get content resource.
             *
             * <p>Create a request for the method "content.get".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the content:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             * @since 1.13
             */
            protected Get(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public Get set$Xgafv(java.lang.String $Xgafv) {
              return (Get) super.set$Xgafv($Xgafv);
            }

            @Override
            public Get setAccessToken(java.lang.String accessToken) {
              return (Get) super.setAccessToken(accessToken);
            }

            @Override
            public Get setAlt(java.lang.String alt) {
              return (Get) super.setAlt(alt);
            }

            @Override
            public Get setCallback(java.lang.String callback) {
              return (Get) super.setCallback(callback);
            }

            @Override
            public Get setFields(java.lang.String fields) {
              return (Get) super.setFields(fields);
            }

            @Override
            public Get setKey(java.lang.String key) {
              return (Get) super.setKey(key);
            }

            @Override
            public Get setOauthToken(java.lang.String oauthToken) {
              return (Get) super.setOauthToken(oauthToken);
            }

            @Override
            public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Get) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Get setQuotaUser(java.lang.String quotaUser) {
              return (Get) super.setQuotaUser(quotaUser);
            }

            @Override
            public Get setUploadType(java.lang.String uploadType) {
              return (Get) super.setUploadType(uploadType);
            }

            @Override
            public Get setUploadProtocol(java.lang.String uploadProtocol) {
              return (Get) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the content:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the content:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the content:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            public Get setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
              this.name = name;
              return this;
            }

            /** Optional. Specify content view to make a partial request. */
            @com.google.api.client.util.Key private java.lang.String view;

            /** Optional. Specify content view to make a partial request. */
            public java.lang.String getView() {
              return view;
            }

            /** Optional. Specify content view to make a partial request. */
            public Get setView(java.lang.String view) {
              this.view = view;
              return this;
            }

            @Override
            public Get set(String parameterName, Object value) {
              return (Get) super.set(parameterName, value);
            }
          }
          /**
           * Gets the access control policy for a resource. Returns an empty policy if the resource
           * exists and does not have a policy set.
           *
           * <p>Create a request for the method "content.getIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being requested. See the
           *     operation documentation for the appropriate value for this field.
           * @return the request
           */
          public GetIamPolicy getIamPolicy(java.lang.String resource) throws java.io.IOException {
            GetIamPolicy result = new GetIamPolicy(resource);
            initialize(result);
            return result;
          }

          public class GetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:getIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");

            /**
             * Gets the access control policy for a resource. Returns an empty policy if the
             * resource exists and does not have a policy set.
             *
             * <p>Create a request for the method "content.getIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * GetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being requested. See
             *     the operation documentation for the appropriate value for this field.
             * @since 1.13
             */
            protected GetIamPolicy(java.lang.String resource) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public GetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (GetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public GetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (GetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public GetIamPolicy setAlt(java.lang.String alt) {
              return (GetIamPolicy) super.setAlt(alt);
            }

            @Override
            public GetIamPolicy setCallback(java.lang.String callback) {
              return (GetIamPolicy) super.setCallback(callback);
            }

            @Override
            public GetIamPolicy setFields(java.lang.String fields) {
              return (GetIamPolicy) super.setFields(fields);
            }

            @Override
            public GetIamPolicy setKey(java.lang.String key) {
              return (GetIamPolicy) super.setKey(key);
            }

            @Override
            public GetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (GetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public GetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (GetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public GetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (GetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public GetIamPolicy setUploadType(java.lang.String uploadType) {
              return (GetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public GetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (GetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public GetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            @com.google.api.client.util.Key("options.requestedPolicyVersion")
            private java.lang.Integer optionsRequestedPolicyVersion;

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public java.lang.Integer getOptionsRequestedPolicyVersion() {
              return optionsRequestedPolicyVersion;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public GetIamPolicy setOptionsRequestedPolicyVersion(
                java.lang.Integer optionsRequestedPolicyVersion) {
              this.optionsRequestedPolicyVersion = optionsRequestedPolicyVersion;
              return this;
            }

            @Override
            public GetIamPolicy set(String parameterName, Object value) {
              return (GetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Lists content under the given lake.
           *
           * <p>Create a request for the method "content.list".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ListContentResponse> {

            private static final String REST_PATH = "v1/{+parent}/content";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Lists content under the given lake.
             *
             * <p>Create a request for the method "content.list".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListContentResponse
                      .class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public List set$Xgafv(java.lang.String $Xgafv) {
              return (List) super.set$Xgafv($Xgafv);
            }

            @Override
            public List setAccessToken(java.lang.String accessToken) {
              return (List) super.setAccessToken(accessToken);
            }

            @Override
            public List setAlt(java.lang.String alt) {
              return (List) super.setAlt(alt);
            }

            @Override
            public List setCallback(java.lang.String callback) {
              return (List) super.setCallback(callback);
            }

            @Override
            public List setFields(java.lang.String fields) {
              return (List) super.setFields(fields);
            }

            @Override
            public List setKey(java.lang.String key) {
              return (List) super.setKey(key);
            }

            @Override
            public List setOauthToken(java.lang.String oauthToken) {
              return (List) super.setOauthToken(oauthToken);
            }

            @Override
            public List setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (List) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public List setQuotaUser(java.lang.String quotaUser) {
              return (List) super.setQuotaUser(quotaUser);
            }

            @Override
            public List setUploadType(java.lang.String uploadType) {
              return (List) super.setUploadType(uploadType);
            }

            @Override
            public List setUploadProtocol(java.lang.String uploadProtocol) {
              return (List) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Optional. Filter request. Filters are case-sensitive. The following formats are
             * supported:path = "/user/directory/notebook1.ipynb" path =
             * starts_with("/my/notebook/directory/file.ipynb") labels.key1 = "value1" labels:key1
             * is_notebook is_sql_script = trueThese restrictions can be coinjoined with AND, OR and
             * NOT conjunctions.
             */
            @com.google.api.client.util.Key private java.lang.String filter;

            /**
             * Optional. Filter request. Filters are case-sensitive. The following formats are
             * supported:path = "/user/directory/notebook1.ipynb" path =
             * starts_with("/my/notebook/directory/file.ipynb") labels.key1 = "value1" labels:key1
             * is_notebook is_sql_script = trueThese restrictions can be coinjoined with AND, OR and
             * NOT conjunctions.
             */
            public java.lang.String getFilter() {
              return filter;
            }

            /**
             * Optional. Filter request. Filters are case-sensitive. The following formats are
             * supported:path = "/user/directory/notebook1.ipynb" path =
             * starts_with("/my/notebook/directory/file.ipynb") labels.key1 = "value1" labels:key1
             * is_notebook is_sql_script = trueThese restrictions can be coinjoined with AND, OR and
             * NOT conjunctions.
             */
            public List setFilter(java.lang.String filter) {
              this.filter = filter;
              return this;
            }

            /**
             * Optional. Maximum number of content to return. The service may return fewer than this
             * value. If unspecified, at most 10 content will be returned. The maximum value is
             * 1000; values above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Optional. Maximum number of content to return. The service may return fewer than this
             * value. If unspecified, at most 10 content will be returned. The maximum value is
             * 1000; values above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Optional. Maximum number of content to return. The service may return fewer than this
             * value. If unspecified, at most 10 content will be returned. The maximum value is
             * 1000; values above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Optional. Page token received from a previous ListContent call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListContent must match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Optional. Page token received from a previous ListContent call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListContent must match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Optional. Page token received from a previous ListContent call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListContent must match the call that provided the page token.
             */
            public List setPageToken(java.lang.String pageToken) {
              this.pageToken = pageToken;
              return this;
            }

            @Override
            public List set(String parameterName, Object value) {
              return (List) super.set(parameterName, value);
            }
          }
          /**
           * Update the content resource.
           *
           * <p>Create a request for the method "content.patch".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * @param name Output only. The relative resource name of the content, of the form:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content}
           * @return the request
           */
          public Patch patch(
              java.lang.String name,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content content)
              throws java.io.IOException {
            Patch result = new Patch(name, content);
            initialize(result);
            return result;
          }

          public class Patch
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");

            /**
             * Update the content resource.
             *
             * <p>Create a request for the method "content.patch".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Output only. The relative resource name of the content, of the form:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content}
             * @since 1.13
             */
            protected Patch(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Content content) {
              super(
                  CloudDataplex.this,
                  "PATCH",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
            }

            @Override
            public Patch set$Xgafv(java.lang.String $Xgafv) {
              return (Patch) super.set$Xgafv($Xgafv);
            }

            @Override
            public Patch setAccessToken(java.lang.String accessToken) {
              return (Patch) super.setAccessToken(accessToken);
            }

            @Override
            public Patch setAlt(java.lang.String alt) {
              return (Patch) super.setAlt(alt);
            }

            @Override
            public Patch setCallback(java.lang.String callback) {
              return (Patch) super.setCallback(callback);
            }

            @Override
            public Patch setFields(java.lang.String fields) {
              return (Patch) super.setFields(fields);
            }

            @Override
            public Patch setKey(java.lang.String key) {
              return (Patch) super.setKey(key);
            }

            @Override
            public Patch setOauthToken(java.lang.String oauthToken) {
              return (Patch) super.setOauthToken(oauthToken);
            }

            @Override
            public Patch setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Patch) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Patch setQuotaUser(java.lang.String quotaUser) {
              return (Patch) super.setQuotaUser(quotaUser);
            }

            @Override
            public Patch setUploadType(java.lang.String uploadType) {
              return (Patch) super.setUploadType(uploadType);
            }

            @Override
            public Patch setUploadProtocol(java.lang.String uploadProtocol) {
              return (Patch) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Output only. The relative resource name of the content, of the form:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Output only. The relative resource name of the content, of the form:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Output only. The relative resource name of the content, of the form:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/content/{content_id}
             */
            public Patch setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
              this.name = name;
              return this;
            }

            /** Required. Mask of fields to update. */
            @com.google.api.client.util.Key private String updateMask;

            /** Required. Mask of fields to update. */
            public String getUpdateMask() {
              return updateMask;
            }

            /** Required. Mask of fields to update. */
            public Patch setUpdateMask(String updateMask) {
              this.updateMask = updateMask;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Patch setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Patch set(String parameterName, Object value) {
              return (Patch) super.set(parameterName, value);
            }
          }
          /**
           * Sets the access control policy on the specified resource. Replaces any existing
           * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
           *
           * <p>Create a request for the method "content.setIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being specified. See the
           *     operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
           * @return the request
           */
          public SetIamPolicy setIamPolicy(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content)
              throws java.io.IOException {
            SetIamPolicy result = new SetIamPolicy(resource, content);
            initialize(result);
            return result;
          }

          public class SetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:setIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");

            /**
             * Sets the access control policy on the specified resource. Replaces any existing
             * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
             *
             * <p>Create a request for the method "content.setIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * SetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being specified. See
             *     the operation documentation for the appropriate value for this field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
             * @since 1.13
             */
            protected SetIamPolicy(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
            }

            @Override
            public SetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (SetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public SetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (SetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public SetIamPolicy setAlt(java.lang.String alt) {
              return (SetIamPolicy) super.setAlt(alt);
            }

            @Override
            public SetIamPolicy setCallback(java.lang.String callback) {
              return (SetIamPolicy) super.setCallback(callback);
            }

            @Override
            public SetIamPolicy setFields(java.lang.String fields) {
              return (SetIamPolicy) super.setFields(fields);
            }

            @Override
            public SetIamPolicy setKey(java.lang.String key) {
              return (SetIamPolicy) super.setKey(key);
            }

            @Override
            public SetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (SetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public SetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (SetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public SetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (SetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public SetIamPolicy setUploadType(java.lang.String uploadType) {
              return (SetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public SetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (SetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public SetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public SetIamPolicy set(String parameterName, Object value) {
              return (SetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Returns permissions that a caller has on the specified resource. If the resource does
           * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
           * This operation is designed to be used for building permission-aware UIs and
           * command-line tools, not for authorization checking. This operation may "fail open"
           * without warning.
           *
           * <p>Create a request for the method "content.testIamPermissions".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link TestIamPermissions#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy detail is being requested.
           *     See the operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
           * @return the request
           */
          public TestIamPermissions testIamPermissions(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                  content)
              throws java.io.IOException {
            TestIamPermissions result = new TestIamPermissions(resource, content);
            initialize(result);
            return result;
          }

          public class TestIamPermissions
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse> {

            private static final String REST_PATH = "v1/{+resource}:testIamPermissions";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");

            /**
             * Returns permissions that a caller has on the specified resource. If the resource does
             * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
             * This operation is designed to be used for building permission-aware UIs and
             * command-line tools, not for authorization checking. This operation may "fail open"
             * without warning.
             *
             * <p>Create a request for the method "content.testIamPermissions".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link TestIamPermissions#execute()} method to
             * invoke the remote operation.
             *
             * <p>{@link TestIamPermissions#initialize(com.google.api.client.googleapis.services.A
             * bstractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param resource REQUIRED: The resource for which the policy detail is being
             *     requested. See the operation documentation for the appropriate value for this
             *     field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
             * @since 1.13
             */
            protected TestIamPermissions(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                    content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse
                      .class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
            }

            @Override
            public TestIamPermissions set$Xgafv(java.lang.String $Xgafv) {
              return (TestIamPermissions) super.set$Xgafv($Xgafv);
            }

            @Override
            public TestIamPermissions setAccessToken(java.lang.String accessToken) {
              return (TestIamPermissions) super.setAccessToken(accessToken);
            }

            @Override
            public TestIamPermissions setAlt(java.lang.String alt) {
              return (TestIamPermissions) super.setAlt(alt);
            }

            @Override
            public TestIamPermissions setCallback(java.lang.String callback) {
              return (TestIamPermissions) super.setCallback(callback);
            }

            @Override
            public TestIamPermissions setFields(java.lang.String fields) {
              return (TestIamPermissions) super.setFields(fields);
            }

            @Override
            public TestIamPermissions setKey(java.lang.String key) {
              return (TestIamPermissions) super.setKey(key);
            }

            @Override
            public TestIamPermissions setOauthToken(java.lang.String oauthToken) {
              return (TestIamPermissions) super.setOauthToken(oauthToken);
            }

            @Override
            public TestIamPermissions setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (TestIamPermissions) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public TestIamPermissions setQuotaUser(java.lang.String quotaUser) {
              return (TestIamPermissions) super.setQuotaUser(quotaUser);
            }

            @Override
            public TestIamPermissions setUploadType(java.lang.String uploadType) {
              return (TestIamPermissions) super.setUploadType(uploadType);
            }

            @Override
            public TestIamPermissions setUploadProtocol(java.lang.String uploadProtocol) {
              return (TestIamPermissions) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public TestIamPermissions setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/content/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public TestIamPermissions set(String parameterName, Object value) {
              return (TestIamPermissions) super.set(parameterName, value);
            }
          }
        }
        /**
         * An accessor for creating requests from the Environments collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
         *   {@code CloudDataplex.Environments.List request = dataplex.environments().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Environments environments() {
          return new Environments();
        }

        /** The "environments" collection of methods. */
        public class Environments {

          /**
           * Create a environment resource.
           *
           * <p>Create a request for the method "environments.create".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment}
           * @return the request
           */
          public Create create(
              java.lang.String parent,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment content)
              throws java.io.IOException {
            Create result = new Create(parent, content);
            initialize(result);
            return result;
          }

          public class Create
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+parent}/environments";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Create a environment resource.
             *
             * <p>Create a request for the method "environments.create".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Create#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment}
             * @since 1.13
             */
            protected Create(
                java.lang.String parent,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment
                    content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public Create set$Xgafv(java.lang.String $Xgafv) {
              return (Create) super.set$Xgafv($Xgafv);
            }

            @Override
            public Create setAccessToken(java.lang.String accessToken) {
              return (Create) super.setAccessToken(accessToken);
            }

            @Override
            public Create setAlt(java.lang.String alt) {
              return (Create) super.setAlt(alt);
            }

            @Override
            public Create setCallback(java.lang.String callback) {
              return (Create) super.setCallback(callback);
            }

            @Override
            public Create setFields(java.lang.String fields) {
              return (Create) super.setFields(fields);
            }

            @Override
            public Create setKey(java.lang.String key) {
              return (Create) super.setKey(key);
            }

            @Override
            public Create setOauthToken(java.lang.String oauthToken) {
              return (Create) super.setOauthToken(oauthToken);
            }

            @Override
            public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Create) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Create setQuotaUser(java.lang.String quotaUser) {
              return (Create) super.setQuotaUser(quotaUser);
            }

            @Override
            public Create setUploadType(java.lang.String uploadType) {
              return (Create) super.setUploadType(uploadType);
            }

            @Override
            public Create setUploadProtocol(java.lang.String uploadProtocol) {
              return (Create) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public Create setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Required. Environment identifier. * Must contain only lowercase letters, numbers and
             * hyphens. * Must start with a letter. * Must be between 1-63 characters. * Must end
             * with a number or a letter. * Must be unique within the lake.
             */
            @com.google.api.client.util.Key private java.lang.String environmentId;

            /**
             * Required. Environment identifier. * Must contain only lowercase letters, numbers and
             * hyphens. * Must start with a letter. * Must be between 1-63 characters. * Must end
             * with a number or a letter. Must be unique within the lake.
             */
            public java.lang.String getEnvironmentId() {
              return environmentId;
            }

            /**
             * Required. Environment identifier. * Must contain only lowercase letters, numbers and
             * hyphens. * Must start with a letter. * Must be between 1-63 characters. * Must end
             * with a number or a letter. * Must be unique within the lake.
             */
            public Create setEnvironmentId(java.lang.String environmentId) {
              this.environmentId = environmentId;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Create setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Create set(String parameterName, Object value) {
              return (Create) super.set(parameterName, value);
            }
          }
          /**
           * Delete the environment resource. All the child resources must have been deleted before
           * environment deletion can be initiated.
           *
           * <p>Create a request for the method "environments.delete".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the environment:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environments/{environment_id
           *     }`
           * @return the request
           */
          public Delete delete(java.lang.String name) throws java.io.IOException {
            Delete result = new Delete(name);
            initialize(result);
            return result;
          }

          public class Delete
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

            /**
             * Delete the environment resource. All the child resources must have been deleted
             * before environment deletion can be initiated.
             *
             * <p>Create a request for the method "environments.delete".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Delete#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the environment:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environments/{environment_id
             *     }`
             * @since 1.13
             */
            protected Delete(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "DELETE",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
            }

            @Override
            public Delete set$Xgafv(java.lang.String $Xgafv) {
              return (Delete) super.set$Xgafv($Xgafv);
            }

            @Override
            public Delete setAccessToken(java.lang.String accessToken) {
              return (Delete) super.setAccessToken(accessToken);
            }

            @Override
            public Delete setAlt(java.lang.String alt) {
              return (Delete) super.setAlt(alt);
            }

            @Override
            public Delete setCallback(java.lang.String callback) {
              return (Delete) super.setCallback(callback);
            }

            @Override
            public Delete setFields(java.lang.String fields) {
              return (Delete) super.setFields(fields);
            }

            @Override
            public Delete setKey(java.lang.String key) {
              return (Delete) super.setKey(key);
            }

            @Override
            public Delete setOauthToken(java.lang.String oauthToken) {
              return (Delete) super.setOauthToken(oauthToken);
            }

            @Override
            public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Delete) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Delete setQuotaUser(java.lang.String quotaUser) {
              return (Delete) super.setQuotaUser(quotaUser);
            }

            @Override
            public Delete setUploadType(java.lang.String uploadType) {
              return (Delete) super.setUploadType(uploadType);
            }

            @Override
            public Delete setUploadProtocol(java.lang.String uploadProtocol) {
              return (Delete) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the environment: projects/{project_id}/locations/{loca
             * tion_id}/lakes/{lake_id}/environments/{environment_id}`
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the environment:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environments/{environment_id}`
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the environment: projects/{project_id}/locations/{loca
             * tion_id}/lakes/{lake_id}/environments/{environment_id}`
             */
            public Delete setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Delete set(String parameterName, Object value) {
              return (Delete) super.set(parameterName, value);
            }
          }
          /**
           * Get environment resource.
           *
           * <p>Create a request for the method "environments.get".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the environment:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environments/{environment_id
           *     }
           * @return the request
           */
          public Get get(java.lang.String name) throws java.io.IOException {
            Get result = new Get(name);
            initialize(result);
            return result;
          }

          public class Get
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

            /**
             * Get environment resource.
             *
             * <p>Create a request for the method "environments.get".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the environment:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environments/{environment_id
             *     }
             * @since 1.13
             */
            protected Get(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public Get set$Xgafv(java.lang.String $Xgafv) {
              return (Get) super.set$Xgafv($Xgafv);
            }

            @Override
            public Get setAccessToken(java.lang.String accessToken) {
              return (Get) super.setAccessToken(accessToken);
            }

            @Override
            public Get setAlt(java.lang.String alt) {
              return (Get) super.setAlt(alt);
            }

            @Override
            public Get setCallback(java.lang.String callback) {
              return (Get) super.setCallback(callback);
            }

            @Override
            public Get setFields(java.lang.String fields) {
              return (Get) super.setFields(fields);
            }

            @Override
            public Get setKey(java.lang.String key) {
              return (Get) super.setKey(key);
            }

            @Override
            public Get setOauthToken(java.lang.String oauthToken) {
              return (Get) super.setOauthToken(oauthToken);
            }

            @Override
            public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Get) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Get setQuotaUser(java.lang.String quotaUser) {
              return (Get) super.setQuotaUser(quotaUser);
            }

            @Override
            public Get setUploadType(java.lang.String uploadType) {
              return (Get) super.setUploadType(uploadType);
            }

            @Override
            public Get setUploadProtocol(java.lang.String uploadProtocol) {
              return (Get) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the environment: projects/{project_id}/locations/{loca
             * tion_id}/lakes/{lake_id}/environments/{environment_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the environment:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environments/{environment_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the environment: projects/{project_id}/locations/{loca
             * tion_id}/lakes/{lake_id}/environments/{environment_id}
             */
            public Get setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Get set(String parameterName, Object value) {
              return (Get) super.set(parameterName, value);
            }
          }
          /**
           * Gets the access control policy for a resource. Returns an empty policy if the resource
           * exists and does not have a policy set.
           *
           * <p>Create a request for the method "environments.getIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being requested. See the
           *     operation documentation for the appropriate value for this field.
           * @return the request
           */
          public GetIamPolicy getIamPolicy(java.lang.String resource) throws java.io.IOException {
            GetIamPolicy result = new GetIamPolicy(resource);
            initialize(result);
            return result;
          }

          public class GetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:getIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

            /**
             * Gets the access control policy for a resource. Returns an empty policy if the
             * resource exists and does not have a policy set.
             *
             * <p>Create a request for the method "environments.getIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * GetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being requested. See
             *     the operation documentation for the appropriate value for this field.
             * @since 1.13
             */
            protected GetIamPolicy(java.lang.String resource) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public GetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (GetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public GetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (GetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public GetIamPolicy setAlt(java.lang.String alt) {
              return (GetIamPolicy) super.setAlt(alt);
            }

            @Override
            public GetIamPolicy setCallback(java.lang.String callback) {
              return (GetIamPolicy) super.setCallback(callback);
            }

            @Override
            public GetIamPolicy setFields(java.lang.String fields) {
              return (GetIamPolicy) super.setFields(fields);
            }

            @Override
            public GetIamPolicy setKey(java.lang.String key) {
              return (GetIamPolicy) super.setKey(key);
            }

            @Override
            public GetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (GetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public GetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (GetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public GetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (GetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public GetIamPolicy setUploadType(java.lang.String uploadType) {
              return (GetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public GetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (GetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public GetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            @com.google.api.client.util.Key("options.requestedPolicyVersion")
            private java.lang.Integer optionsRequestedPolicyVersion;

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public java.lang.Integer getOptionsRequestedPolicyVersion() {
              return optionsRequestedPolicyVersion;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public GetIamPolicy setOptionsRequestedPolicyVersion(
                java.lang.Integer optionsRequestedPolicyVersion) {
              this.optionsRequestedPolicyVersion = optionsRequestedPolicyVersion;
              return this;
            }

            @Override
            public GetIamPolicy set(String parameterName, Object value) {
              return (GetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Lists environments under the given lake.
           *
           * <p>Create a request for the method "environments.list".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ListEnvironmentsResponse> {

            private static final String REST_PATH = "v1/{+parent}/environments";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Lists environments under the given lake.
             *
             * <p>Create a request for the method "environments.list".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ListEnvironmentsResponse.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public List set$Xgafv(java.lang.String $Xgafv) {
              return (List) super.set$Xgafv($Xgafv);
            }

            @Override
            public List setAccessToken(java.lang.String accessToken) {
              return (List) super.setAccessToken(accessToken);
            }

            @Override
            public List setAlt(java.lang.String alt) {
              return (List) super.setAlt(alt);
            }

            @Override
            public List setCallback(java.lang.String callback) {
              return (List) super.setCallback(callback);
            }

            @Override
            public List setFields(java.lang.String fields) {
              return (List) super.setFields(fields);
            }

            @Override
            public List setKey(java.lang.String key) {
              return (List) super.setKey(key);
            }

            @Override
            public List setOauthToken(java.lang.String oauthToken) {
              return (List) super.setOauthToken(oauthToken);
            }

            @Override
            public List setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (List) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public List setQuotaUser(java.lang.String quotaUser) {
              return (List) super.setQuotaUser(quotaUser);
            }

            @Override
            public List setUploadType(java.lang.String uploadType) {
              return (List) super.setUploadType(uploadType);
            }

            @Override
            public List setUploadProtocol(java.lang.String uploadProtocol) {
              return (List) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}
             */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /** Optional. Filter request. */
            @com.google.api.client.util.Key private java.lang.String filter;

            /** Optional. Filter request. */
            public java.lang.String getFilter() {
              return filter;
            }

            /** Optional. Filter request. */
            public List setFilter(java.lang.String filter) {
              this.filter = filter;
              return this;
            }

            /** Optional. Order by fields for the result. */
            @com.google.api.client.util.Key private java.lang.String orderBy;

            /** Optional. Order by fields for the result. */
            public java.lang.String getOrderBy() {
              return orderBy;
            }

            /** Optional. Order by fields for the result. */
            public List setOrderBy(java.lang.String orderBy) {
              this.orderBy = orderBy;
              return this;
            }

            /**
             * Optional. Maximum number of environments to return. The service may return fewer than
             * this value. If unspecified, at most 10 environments will be returned. The maximum
             * value is 1000; values above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Optional. Maximum number of environments to return. The service may return fewer than
             * this value. If unspecified, at most 10 environments will be returned. The maximum
             * value is 1000; values above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Optional. Maximum number of environments to return. The service may return fewer than
             * this value. If unspecified, at most 10 environments will be returned. The maximum
             * value is 1000; values above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Optional. Page token received from a previous ListEnvironments call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListEnvironments must match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Optional. Page token received from a previous ListEnvironments call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListEnvironments must match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Optional. Page token received from a previous ListEnvironments call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListEnvironments must match the call that provided the page token.
             */
            public List setPageToken(java.lang.String pageToken) {
              this.pageToken = pageToken;
              return this;
            }

            @Override
            public List set(String parameterName, Object value) {
              return (List) super.set(parameterName, value);
            }
          }
          /**
           * Update the environment resource.
           *
           * <p>Create a request for the method "environments.patch".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * @param name Output only. The relative resource name of the environment, of the form:
           *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment}
           * @return the request
           */
          public Patch patch(
              java.lang.String name,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment content)
              throws java.io.IOException {
            Patch result = new Patch(name, content);
            initialize(result);
            return result;
          }

          public class Patch
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

            /**
             * Update the environment resource.
             *
             * <p>Create a request for the method "environments.patch".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Output only. The relative resource name of the environment, of the form:
             *     projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment}
             * @since 1.13
             */
            protected Patch(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Environment
                    content) {
              super(
                  CloudDataplex.this,
                  "PATCH",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
            }

            @Override
            public Patch set$Xgafv(java.lang.String $Xgafv) {
              return (Patch) super.set$Xgafv($Xgafv);
            }

            @Override
            public Patch setAccessToken(java.lang.String accessToken) {
              return (Patch) super.setAccessToken(accessToken);
            }

            @Override
            public Patch setAlt(java.lang.String alt) {
              return (Patch) super.setAlt(alt);
            }

            @Override
            public Patch setCallback(java.lang.String callback) {
              return (Patch) super.setCallback(callback);
            }

            @Override
            public Patch setFields(java.lang.String fields) {
              return (Patch) super.setFields(fields);
            }

            @Override
            public Patch setKey(java.lang.String key) {
              return (Patch) super.setKey(key);
            }

            @Override
            public Patch setOauthToken(java.lang.String oauthToken) {
              return (Patch) super.setOauthToken(oauthToken);
            }

            @Override
            public Patch setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Patch) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Patch setQuotaUser(java.lang.String quotaUser) {
              return (Patch) super.setQuotaUser(quotaUser);
            }

            @Override
            public Patch setUploadType(java.lang.String uploadType) {
              return (Patch) super.setUploadType(uploadType);
            }

            @Override
            public Patch setUploadProtocol(java.lang.String uploadProtocol) {
              return (Patch) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Output only. The relative resource name of the environment, of the form: projects/{pr
             * oject_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Output only. The relative resource name of the environment, of the form:
             * projects/{project_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Output only. The relative resource name of the environment, of the form: projects/{pr
             * oject_id}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
             */
            public Patch setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
              this.name = name;
              return this;
            }

            /** Required. Mask of fields to update. */
            @com.google.api.client.util.Key private String updateMask;

            /** Required. Mask of fields to update. */
            public String getUpdateMask() {
              return updateMask;
            }

            /** Required. Mask of fields to update. */
            public Patch setUpdateMask(String updateMask) {
              this.updateMask = updateMask;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Patch setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Patch set(String parameterName, Object value) {
              return (Patch) super.set(parameterName, value);
            }
          }
          /**
           * Sets the access control policy on the specified resource. Replaces any existing
           * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
           *
           * <p>Create a request for the method "environments.setIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being specified. See the
           *     operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
           * @return the request
           */
          public SetIamPolicy setIamPolicy(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content)
              throws java.io.IOException {
            SetIamPolicy result = new SetIamPolicy(resource, content);
            initialize(result);
            return result;
          }

          public class SetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:setIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

            /**
             * Sets the access control policy on the specified resource. Replaces any existing
             * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
             *
             * <p>Create a request for the method "environments.setIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * SetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being specified. See
             *     the operation documentation for the appropriate value for this field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
             * @since 1.13
             */
            protected SetIamPolicy(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
            }

            @Override
            public SetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (SetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public SetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (SetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public SetIamPolicy setAlt(java.lang.String alt) {
              return (SetIamPolicy) super.setAlt(alt);
            }

            @Override
            public SetIamPolicy setCallback(java.lang.String callback) {
              return (SetIamPolicy) super.setCallback(callback);
            }

            @Override
            public SetIamPolicy setFields(java.lang.String fields) {
              return (SetIamPolicy) super.setFields(fields);
            }

            @Override
            public SetIamPolicy setKey(java.lang.String key) {
              return (SetIamPolicy) super.setKey(key);
            }

            @Override
            public SetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (SetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public SetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (SetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public SetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (SetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public SetIamPolicy setUploadType(java.lang.String uploadType) {
              return (SetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public SetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (SetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public SetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public SetIamPolicy set(String parameterName, Object value) {
              return (SetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Returns permissions that a caller has on the specified resource. If the resource does
           * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
           * This operation is designed to be used for building permission-aware UIs and
           * command-line tools, not for authorization checking. This operation may "fail open"
           * without warning.
           *
           * <p>Create a request for the method "environments.testIamPermissions".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link TestIamPermissions#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy detail is being requested.
           *     See the operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
           * @return the request
           */
          public TestIamPermissions testIamPermissions(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                  content)
              throws java.io.IOException {
            TestIamPermissions result = new TestIamPermissions(resource, content);
            initialize(result);
            return result;
          }

          public class TestIamPermissions
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse> {

            private static final String REST_PATH = "v1/{+resource}:testIamPermissions";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

            /**
             * Returns permissions that a caller has on the specified resource. If the resource does
             * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
             * This operation is designed to be used for building permission-aware UIs and
             * command-line tools, not for authorization checking. This operation may "fail open"
             * without warning.
             *
             * <p>Create a request for the method "environments.testIamPermissions".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link TestIamPermissions#execute()} method to
             * invoke the remote operation.
             *
             * <p>{@link TestIamPermissions#initialize(com.google.api.client.googleapis.services.A
             * bstractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param resource REQUIRED: The resource for which the policy detail is being
             *     requested. See the operation documentation for the appropriate value for this
             *     field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
             * @since 1.13
             */
            protected TestIamPermissions(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                    content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse
                      .class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
            }

            @Override
            public TestIamPermissions set$Xgafv(java.lang.String $Xgafv) {
              return (TestIamPermissions) super.set$Xgafv($Xgafv);
            }

            @Override
            public TestIamPermissions setAccessToken(java.lang.String accessToken) {
              return (TestIamPermissions) super.setAccessToken(accessToken);
            }

            @Override
            public TestIamPermissions setAlt(java.lang.String alt) {
              return (TestIamPermissions) super.setAlt(alt);
            }

            @Override
            public TestIamPermissions setCallback(java.lang.String callback) {
              return (TestIamPermissions) super.setCallback(callback);
            }

            @Override
            public TestIamPermissions setFields(java.lang.String fields) {
              return (TestIamPermissions) super.setFields(fields);
            }

            @Override
            public TestIamPermissions setKey(java.lang.String key) {
              return (TestIamPermissions) super.setKey(key);
            }

            @Override
            public TestIamPermissions setOauthToken(java.lang.String oauthToken) {
              return (TestIamPermissions) super.setOauthToken(oauthToken);
            }

            @Override
            public TestIamPermissions setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (TestIamPermissions) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public TestIamPermissions setQuotaUser(java.lang.String quotaUser) {
              return (TestIamPermissions) super.setQuotaUser(quotaUser);
            }

            @Override
            public TestIamPermissions setUploadType(java.lang.String uploadType) {
              return (TestIamPermissions) super.setUploadType(uploadType);
            }

            @Override
            public TestIamPermissions setUploadProtocol(java.lang.String uploadProtocol) {
              return (TestIamPermissions) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public TestIamPermissions setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public TestIamPermissions set(String parameterName, Object value) {
              return (TestIamPermissions) super.set(parameterName, value);
            }
          }

          /**
           * An accessor for creating requests from the Sessions collection.
           *
           * <p>The typical use is:
           *
           * <pre>
           *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
           *   {@code CloudDataplex.Sessions.List request = dataplex.sessions().list(parameters ...)}
           * </pre>
           *
           * @return the resource collection
           */
          public Sessions sessions() {
            return new Sessions();
          }

          /** The "sessions" collection of methods. */
          public class Sessions {

            /**
             * Lists session resources in an environment.
             *
             * <p>Create a request for the method "sessions.list".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent environment:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/environment/{environment
             *     _id}
             * @return the request
             */
            public List list(java.lang.String parent) throws java.io.IOException {
              List result = new List(parent);
              initialize(result);
              return result;
            }

            public class List
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListSessionsResponse> {

              private static final String REST_PATH = "v1/{+parent}/sessions";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");

              /**
               * Lists session resources in an environment.
               *
               * <p>Create a request for the method "sessions.list".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link List#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent environment:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/environment/{environment
               *     _id}
               * @since 1.13
               */
              protected List(java.lang.String parent) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListSessionsResponse.class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public List set$Xgafv(java.lang.String $Xgafv) {
                return (List) super.set$Xgafv($Xgafv);
              }

              @Override
              public List setAccessToken(java.lang.String accessToken) {
                return (List) super.setAccessToken(accessToken);
              }

              @Override
              public List setAlt(java.lang.String alt) {
                return (List) super.setAlt(alt);
              }

              @Override
              public List setCallback(java.lang.String callback) {
                return (List) super.setCallback(callback);
              }

              @Override
              public List setFields(java.lang.String fields) {
                return (List) super.setFields(fields);
              }

              @Override
              public List setKey(java.lang.String key) {
                return (List) super.setKey(key);
              }

              @Override
              public List setOauthToken(java.lang.String oauthToken) {
                return (List) super.setOauthToken(oauthToken);
              }

              @Override
              public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (List) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public List setQuotaUser(java.lang.String quotaUser) {
                return (List) super.setQuotaUser(quotaUser);
              }

              @Override
              public List setUploadType(java.lang.String uploadType) {
                return (List) super.setUploadType(uploadType);
              }

              @Override
              public List setUploadProtocol(java.lang.String uploadProtocol) {
                return (List) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent environment: projects/{project_number}/lo
               * cations/{location_id}/lakes/{lake_id}/environment/{environment_id}
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent environment:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/environment/{environment_id}
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent environment: projects/{project_number}/lo
               * cations/{location_id}/lakes/{lake_id}/environment/{environment_id}
               */
              public List setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/environments/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /**
               * Optional. Maximum number of sessions to return. The service may return fewer than
               * this value. If unspecified, at most 10 sessions will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              @com.google.api.client.util.Key private java.lang.Integer pageSize;

              /**
               * Optional. Maximum number of sessions to return. The service may return fewer than
               * this value. If unspecified, at most 10 sessions will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              public java.lang.Integer getPageSize() {
                return pageSize;
              }

              /**
               * Optional. Maximum number of sessions to return. The service may return fewer than
               * this value. If unspecified, at most 10 sessions will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              public List setPageSize(java.lang.Integer pageSize) {
                this.pageSize = pageSize;
                return this;
              }

              /**
               * Optional. Page token received from a previous ListSessions call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListSessions must match the call that provided the page token.
               */
              @com.google.api.client.util.Key private java.lang.String pageToken;

              /**
               * Optional. Page token received from a previous ListSessions call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListSessions must match the call that provided the page token.
               */
              public java.lang.String getPageToken() {
                return pageToken;
              }

              /**
               * Optional. Page token received from a previous ListSessions call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListSessions must match the call that provided the page token.
               */
              public List setPageToken(java.lang.String pageToken) {
                this.pageToken = pageToken;
                return this;
              }

              @Override
              public List set(String parameterName, Object value) {
                return (List) super.set(parameterName, value);
              }
            }
          }
        }
        /**
         * An accessor for creating requests from the Tasks collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
         *   {@code CloudDataplex.Tasks.List request = dataplex.tasks().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Tasks tasks() {
          return new Tasks();
        }

        /** The "tasks" collection of methods. */
        public class Tasks {

          /**
           * Creates a task resource within a lake.
           *
           * <p>Create a request for the method "tasks.create".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task}
           * @return the request
           */
          public Create create(
              java.lang.String parent,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task content)
              throws java.io.IOException {
            Create result = new Create(parent, content);
            initialize(result);
            return result;
          }

          public class Create
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+parent}/tasks";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Creates a task resource within a lake.
             *
             * <p>Create a request for the method "tasks.create".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Create#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task}
             * @since 1.13
             */
            protected Create(
                java.lang.String parent,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public Create set$Xgafv(java.lang.String $Xgafv) {
              return (Create) super.set$Xgafv($Xgafv);
            }

            @Override
            public Create setAccessToken(java.lang.String accessToken) {
              return (Create) super.setAccessToken(accessToken);
            }

            @Override
            public Create setAlt(java.lang.String alt) {
              return (Create) super.setAlt(alt);
            }

            @Override
            public Create setCallback(java.lang.String callback) {
              return (Create) super.setCallback(callback);
            }

            @Override
            public Create setFields(java.lang.String fields) {
              return (Create) super.setFields(fields);
            }

            @Override
            public Create setKey(java.lang.String key) {
              return (Create) super.setKey(key);
            }

            @Override
            public Create setOauthToken(java.lang.String oauthToken) {
              return (Create) super.setOauthToken(oauthToken);
            }

            @Override
            public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Create) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Create setQuotaUser(java.lang.String quotaUser) {
              return (Create) super.setQuotaUser(quotaUser);
            }

            @Override
            public Create setUploadType(java.lang.String uploadType) {
              return (Create) super.setUploadType(uploadType);
            }

            @Override
            public Create setUploadProtocol(java.lang.String uploadProtocol) {
              return (Create) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public Create setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /** Required. Task identifier. */
            @com.google.api.client.util.Key private java.lang.String taskId;

            /** Required. Task identifier. */
            public java.lang.String getTaskId() {
              return taskId;
            }

            /** Required. Task identifier. */
            public Create setTaskId(java.lang.String taskId) {
              this.taskId = taskId;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Create setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Create set(String parameterName, Object value) {
              return (Create) super.set(parameterName, value);
            }
          }
          /**
           * Delete the task resource.
           *
           * <p>Create a request for the method "tasks.delete".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the task:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id} /task/{task_id}`
           * @return the request
           */
          public Delete delete(java.lang.String name) throws java.io.IOException {
            Delete result = new Delete(name);
            initialize(result);
            return result;
          }

          public class Delete
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

            /**
             * Delete the task resource.
             *
             * <p>Create a request for the method "tasks.delete".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Delete#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the task:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /task/{task_id}`
             * @since 1.13
             */
            protected Delete(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "DELETE",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
            }

            @Override
            public Delete set$Xgafv(java.lang.String $Xgafv) {
              return (Delete) super.set$Xgafv($Xgafv);
            }

            @Override
            public Delete setAccessToken(java.lang.String accessToken) {
              return (Delete) super.setAccessToken(accessToken);
            }

            @Override
            public Delete setAlt(java.lang.String alt) {
              return (Delete) super.setAlt(alt);
            }

            @Override
            public Delete setCallback(java.lang.String callback) {
              return (Delete) super.setCallback(callback);
            }

            @Override
            public Delete setFields(java.lang.String fields) {
              return (Delete) super.setFields(fields);
            }

            @Override
            public Delete setKey(java.lang.String key) {
              return (Delete) super.setKey(key);
            }

            @Override
            public Delete setOauthToken(java.lang.String oauthToken) {
              return (Delete) super.setOauthToken(oauthToken);
            }

            @Override
            public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Delete) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Delete setQuotaUser(java.lang.String quotaUser) {
              return (Delete) super.setQuotaUser(quotaUser);
            }

            @Override
            public Delete setUploadType(java.lang.String uploadType) {
              return (Delete) super.setUploadType(uploadType);
            }

            @Override
            public Delete setUploadProtocol(java.lang.String uploadProtocol) {
              return (Delete) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the task:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /task/{task_id}`
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the task:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /task/{task_id}`
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the task:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /task/{task_id}`
             */
            public Delete setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Delete set(String parameterName, Object value) {
              return (Delete) super.set(parameterName, value);
            }
          }
          /**
           * Get task resource.
           *
           * <p>Create a request for the method "tasks.get".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the task:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id} /tasks/{tasks_id}
           * @return the request
           */
          public Get get(java.lang.String name) throws java.io.IOException {
            Get result = new Get(name);
            initialize(result);
            return result;
          }

          public class Get
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

            /**
             * Get task resource.
             *
             * <p>Create a request for the method "tasks.get".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the task:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /tasks/{tasks_id}
             * @since 1.13
             */
            protected Get(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public Get set$Xgafv(java.lang.String $Xgafv) {
              return (Get) super.set$Xgafv($Xgafv);
            }

            @Override
            public Get setAccessToken(java.lang.String accessToken) {
              return (Get) super.setAccessToken(accessToken);
            }

            @Override
            public Get setAlt(java.lang.String alt) {
              return (Get) super.setAlt(alt);
            }

            @Override
            public Get setCallback(java.lang.String callback) {
              return (Get) super.setCallback(callback);
            }

            @Override
            public Get setFields(java.lang.String fields) {
              return (Get) super.setFields(fields);
            }

            @Override
            public Get setKey(java.lang.String key) {
              return (Get) super.setKey(key);
            }

            @Override
            public Get setOauthToken(java.lang.String oauthToken) {
              return (Get) super.setOauthToken(oauthToken);
            }

            @Override
            public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Get) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Get setQuotaUser(java.lang.String quotaUser) {
              return (Get) super.setQuotaUser(quotaUser);
            }

            @Override
            public Get setUploadType(java.lang.String uploadType) {
              return (Get) super.setUploadType(uploadType);
            }

            @Override
            public Get setUploadProtocol(java.lang.String uploadProtocol) {
              return (Get) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the task:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /tasks/{tasks_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the task:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /tasks/{tasks_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the task:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /tasks/{tasks_id}
             */
            public Get setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Get set(String parameterName, Object value) {
              return (Get) super.set(parameterName, value);
            }
          }
          /**
           * Gets the access control policy for a resource. Returns an empty policy if the resource
           * exists and does not have a policy set.
           *
           * <p>Create a request for the method "tasks.getIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being requested. See the
           *     operation documentation for the appropriate value for this field.
           * @return the request
           */
          public GetIamPolicy getIamPolicy(java.lang.String resource) throws java.io.IOException {
            GetIamPolicy result = new GetIamPolicy(resource);
            initialize(result);
            return result;
          }

          public class GetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:getIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

            /**
             * Gets the access control policy for a resource. Returns an empty policy if the
             * resource exists and does not have a policy set.
             *
             * <p>Create a request for the method "tasks.getIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * GetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being requested. See
             *     the operation documentation for the appropriate value for this field.
             * @since 1.13
             */
            protected GetIamPolicy(java.lang.String resource) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public GetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (GetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public GetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (GetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public GetIamPolicy setAlt(java.lang.String alt) {
              return (GetIamPolicy) super.setAlt(alt);
            }

            @Override
            public GetIamPolicy setCallback(java.lang.String callback) {
              return (GetIamPolicy) super.setCallback(callback);
            }

            @Override
            public GetIamPolicy setFields(java.lang.String fields) {
              return (GetIamPolicy) super.setFields(fields);
            }

            @Override
            public GetIamPolicy setKey(java.lang.String key) {
              return (GetIamPolicy) super.setKey(key);
            }

            @Override
            public GetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (GetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public GetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (GetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public GetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (GetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public GetIamPolicy setUploadType(java.lang.String uploadType) {
              return (GetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public GetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (GetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public GetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            @com.google.api.client.util.Key("options.requestedPolicyVersion")
            private java.lang.Integer optionsRequestedPolicyVersion;

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public java.lang.Integer getOptionsRequestedPolicyVersion() {
              return optionsRequestedPolicyVersion;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public GetIamPolicy setOptionsRequestedPolicyVersion(
                java.lang.Integer optionsRequestedPolicyVersion) {
              this.optionsRequestedPolicyVersion = optionsRequestedPolicyVersion;
              return this;
            }

            @Override
            public GetIamPolicy set(String parameterName, Object value) {
              return (GetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Lists tasks under the given lake.
           *
           * <p>Create a request for the method "tasks.list".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ListTasksResponse> {

            private static final String REST_PATH = "v1/{+parent}/tasks";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Lists tasks under the given lake.
             *
             * <p>Create a request for the method "tasks.list".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListTasksResponse
                      .class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public List set$Xgafv(java.lang.String $Xgafv) {
              return (List) super.set$Xgafv($Xgafv);
            }

            @Override
            public List setAccessToken(java.lang.String accessToken) {
              return (List) super.setAccessToken(accessToken);
            }

            @Override
            public List setAlt(java.lang.String alt) {
              return (List) super.setAlt(alt);
            }

            @Override
            public List setCallback(java.lang.String callback) {
              return (List) super.setCallback(callback);
            }

            @Override
            public List setFields(java.lang.String fields) {
              return (List) super.setFields(fields);
            }

            @Override
            public List setKey(java.lang.String key) {
              return (List) super.setKey(key);
            }

            @Override
            public List setOauthToken(java.lang.String oauthToken) {
              return (List) super.setOauthToken(oauthToken);
            }

            @Override
            public List setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (List) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public List setQuotaUser(java.lang.String quotaUser) {
              return (List) super.setQuotaUser(quotaUser);
            }

            @Override
            public List setUploadType(java.lang.String uploadType) {
              return (List) super.setUploadType(uploadType);
            }

            @Override
            public List setUploadProtocol(java.lang.String uploadProtocol) {
              return (List) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /** Optional. Filter request. */
            @com.google.api.client.util.Key private java.lang.String filter;

            /** Optional. Filter request. */
            public java.lang.String getFilter() {
              return filter;
            }

            /** Optional. Filter request. */
            public List setFilter(java.lang.String filter) {
              this.filter = filter;
              return this;
            }

            /** Optional. Order by fields for the result. */
            @com.google.api.client.util.Key private java.lang.String orderBy;

            /** Optional. Order by fields for the result. */
            public java.lang.String getOrderBy() {
              return orderBy;
            }

            /** Optional. Order by fields for the result. */
            public List setOrderBy(java.lang.String orderBy) {
              this.orderBy = orderBy;
              return this;
            }

            /**
             * Optional. Maximum number of tasks to return. The service may return fewer than this
             * value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000;
             * values above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Optional. Maximum number of tasks to return. The service may return fewer than this
             * value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000;
             * values above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Optional. Maximum number of tasks to return. The service may return fewer than this
             * value. If unspecified, at most 10 tasks will be returned. The maximum value is 1000;
             * values above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Optional. Page token received from a previous ListZones call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListZones must match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Optional. Page token received from a previous ListZones call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListZones must match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Optional. Page token received from a previous ListZones call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListZones must match the call that provided the page token.
             */
            public List setPageToken(java.lang.String pageToken) {
              this.pageToken = pageToken;
              return this;
            }

            @Override
            public List set(String parameterName, Object value) {
              return (List) super.set(parameterName, value);
            }
          }
          /**
           * Update the task resource.
           *
           * <p>Create a request for the method "tasks.patch".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * @param name Output only. The relative resource name of the task, of the form:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task}
           * @return the request
           */
          public Patch patch(
              java.lang.String name,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task content)
              throws java.io.IOException {
            Patch result = new Patch(name, content);
            initialize(result);
            return result;
          }

          public class Patch
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

            /**
             * Update the task resource.
             *
             * <p>Create a request for the method "tasks.patch".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Output only. The relative resource name of the task, of the form:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/
             *     tasks/{task_id}.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task}
             * @since 1.13
             */
            protected Patch(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Task content) {
              super(
                  CloudDataplex.this,
                  "PATCH",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
            }

            @Override
            public Patch set$Xgafv(java.lang.String $Xgafv) {
              return (Patch) super.set$Xgafv($Xgafv);
            }

            @Override
            public Patch setAccessToken(java.lang.String accessToken) {
              return (Patch) super.setAccessToken(accessToken);
            }

            @Override
            public Patch setAlt(java.lang.String alt) {
              return (Patch) super.setAlt(alt);
            }

            @Override
            public Patch setCallback(java.lang.String callback) {
              return (Patch) super.setCallback(callback);
            }

            @Override
            public Patch setFields(java.lang.String fields) {
              return (Patch) super.setFields(fields);
            }

            @Override
            public Patch setKey(java.lang.String key) {
              return (Patch) super.setKey(key);
            }

            @Override
            public Patch setOauthToken(java.lang.String oauthToken) {
              return (Patch) super.setOauthToken(oauthToken);
            }

            @Override
            public Patch setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Patch) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Patch setQuotaUser(java.lang.String quotaUser) {
              return (Patch) super.setQuotaUser(quotaUser);
            }

            @Override
            public Patch setUploadType(java.lang.String uploadType) {
              return (Patch) super.setUploadType(uploadType);
            }

            @Override
            public Patch setUploadProtocol(java.lang.String uploadProtocol) {
              return (Patch) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Output only. The relative resource name of the task, of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}.
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Output only. The relative resource name of the task, of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}.
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Output only. The relative resource name of the task, of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/ tasks/{task_id}.
             */
            public Patch setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
              this.name = name;
              return this;
            }

            /** Required. Mask of fields to update. */
            @com.google.api.client.util.Key private String updateMask;

            /** Required. Mask of fields to update. */
            public String getUpdateMask() {
              return updateMask;
            }

            /** Required. Mask of fields to update. */
            public Patch setUpdateMask(String updateMask) {
              this.updateMask = updateMask;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Patch setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Patch set(String parameterName, Object value) {
              return (Patch) super.set(parameterName, value);
            }
          }
          /**
           * Sets the access control policy on the specified resource. Replaces any existing
           * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
           *
           * <p>Create a request for the method "tasks.setIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being specified. See the
           *     operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
           * @return the request
           */
          public SetIamPolicy setIamPolicy(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content)
              throws java.io.IOException {
            SetIamPolicy result = new SetIamPolicy(resource, content);
            initialize(result);
            return result;
          }

          public class SetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:setIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

            /**
             * Sets the access control policy on the specified resource. Replaces any existing
             * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
             *
             * <p>Create a request for the method "tasks.setIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * SetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being specified. See
             *     the operation documentation for the appropriate value for this field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
             * @since 1.13
             */
            protected SetIamPolicy(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
            }

            @Override
            public SetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (SetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public SetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (SetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public SetIamPolicy setAlt(java.lang.String alt) {
              return (SetIamPolicy) super.setAlt(alt);
            }

            @Override
            public SetIamPolicy setCallback(java.lang.String callback) {
              return (SetIamPolicy) super.setCallback(callback);
            }

            @Override
            public SetIamPolicy setFields(java.lang.String fields) {
              return (SetIamPolicy) super.setFields(fields);
            }

            @Override
            public SetIamPolicy setKey(java.lang.String key) {
              return (SetIamPolicy) super.setKey(key);
            }

            @Override
            public SetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (SetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public SetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (SetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public SetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (SetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public SetIamPolicy setUploadType(java.lang.String uploadType) {
              return (SetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public SetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (SetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public SetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public SetIamPolicy set(String parameterName, Object value) {
              return (SetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Returns permissions that a caller has on the specified resource. If the resource does
           * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
           * This operation is designed to be used for building permission-aware UIs and
           * command-line tools, not for authorization checking. This operation may "fail open"
           * without warning.
           *
           * <p>Create a request for the method "tasks.testIamPermissions".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link TestIamPermissions#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy detail is being requested.
           *     See the operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
           * @return the request
           */
          public TestIamPermissions testIamPermissions(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                  content)
              throws java.io.IOException {
            TestIamPermissions result = new TestIamPermissions(resource, content);
            initialize(result);
            return result;
          }

          public class TestIamPermissions
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse> {

            private static final String REST_PATH = "v1/{+resource}:testIamPermissions";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

            /**
             * Returns permissions that a caller has on the specified resource. If the resource does
             * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
             * This operation is designed to be used for building permission-aware UIs and
             * command-line tools, not for authorization checking. This operation may "fail open"
             * without warning.
             *
             * <p>Create a request for the method "tasks.testIamPermissions".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link TestIamPermissions#execute()} method to
             * invoke the remote operation.
             *
             * <p>{@link TestIamPermissions#initialize(com.google.api.client.googleapis.services.A
             * bstractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param resource REQUIRED: The resource for which the policy detail is being
             *     requested. See the operation documentation for the appropriate value for this
             *     field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
             * @since 1.13
             */
            protected TestIamPermissions(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                    content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse
                      .class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
            }

            @Override
            public TestIamPermissions set$Xgafv(java.lang.String $Xgafv) {
              return (TestIamPermissions) super.set$Xgafv($Xgafv);
            }

            @Override
            public TestIamPermissions setAccessToken(java.lang.String accessToken) {
              return (TestIamPermissions) super.setAccessToken(accessToken);
            }

            @Override
            public TestIamPermissions setAlt(java.lang.String alt) {
              return (TestIamPermissions) super.setAlt(alt);
            }

            @Override
            public TestIamPermissions setCallback(java.lang.String callback) {
              return (TestIamPermissions) super.setCallback(callback);
            }

            @Override
            public TestIamPermissions setFields(java.lang.String fields) {
              return (TestIamPermissions) super.setFields(fields);
            }

            @Override
            public TestIamPermissions setKey(java.lang.String key) {
              return (TestIamPermissions) super.setKey(key);
            }

            @Override
            public TestIamPermissions setOauthToken(java.lang.String oauthToken) {
              return (TestIamPermissions) super.setOauthToken(oauthToken);
            }

            @Override
            public TestIamPermissions setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (TestIamPermissions) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public TestIamPermissions setQuotaUser(java.lang.String quotaUser) {
              return (TestIamPermissions) super.setQuotaUser(quotaUser);
            }

            @Override
            public TestIamPermissions setUploadType(java.lang.String uploadType) {
              return (TestIamPermissions) super.setUploadType(uploadType);
            }

            @Override
            public TestIamPermissions setUploadProtocol(java.lang.String uploadProtocol) {
              return (TestIamPermissions) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public TestIamPermissions setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public TestIamPermissions set(String parameterName, Object value) {
              return (TestIamPermissions) super.set(parameterName, value);
            }
          }

          /**
           * An accessor for creating requests from the Jobs collection.
           *
           * <p>The typical use is:
           *
           * <pre>
           *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
           *   {@code CloudDataplex.Jobs.List request = dataplex.jobs().list(parameters ...)}
           * </pre>
           *
           * @return the resource collection
           */
          public Jobs jobs() {
            return new Jobs();
          }

          /** The "jobs" collection of methods. */
          public class Jobs {

            /**
             * Cancel jobs running for the task resource.
             *
             * <p>Create a request for the method "jobs.cancel".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Cancel#execute()} method to invoke the remote
             * operation.
             *
             * @param name Required. The resource name of the job:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /task/{task_id}/job/{job_id}`
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1CancelJobRequest}
             * @return the request
             */
            public Cancel cancel(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1CancelJobRequest
                    content)
                throws java.io.IOException {
              Cancel result = new Cancel(name, content);
              initialize(result);
              return result;
            }

            public class Cancel
                extends CloudDataplexRequest<com.google.api.services.dataplex.v1.model.Empty> {

              private static final String REST_PATH = "v1/{+name}:cancel";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+/jobs/[^/]+$");

              /**
               * Cancel jobs running for the task resource.
               *
               * <p>Create a request for the method "jobs.cancel".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Cancel#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Cancel#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Required. The resource name of the job:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /task/{task_id}/job/{job_id}`
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1CancelJobRequest}
               * @since 1.13
               */
              protected Cancel(
                  java.lang.String name,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1CancelJobRequest
                      content) {
                super(
                    CloudDataplex.this,
                    "POST",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.Empty.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+/jobs/[^/]+$");
                }
              }

              @Override
              public Cancel set$Xgafv(java.lang.String $Xgafv) {
                return (Cancel) super.set$Xgafv($Xgafv);
              }

              @Override
              public Cancel setAccessToken(java.lang.String accessToken) {
                return (Cancel) super.setAccessToken(accessToken);
              }

              @Override
              public Cancel setAlt(java.lang.String alt) {
                return (Cancel) super.setAlt(alt);
              }

              @Override
              public Cancel setCallback(java.lang.String callback) {
                return (Cancel) super.setCallback(callback);
              }

              @Override
              public Cancel setFields(java.lang.String fields) {
                return (Cancel) super.setFields(fields);
              }

              @Override
              public Cancel setKey(java.lang.String key) {
                return (Cancel) super.setKey(key);
              }

              @Override
              public Cancel setOauthToken(java.lang.String oauthToken) {
                return (Cancel) super.setOauthToken(oauthToken);
              }

              @Override
              public Cancel setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Cancel) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Cancel setQuotaUser(java.lang.String quotaUser) {
                return (Cancel) super.setQuotaUser(quotaUser);
              }

              @Override
              public Cancel setUploadType(java.lang.String uploadType) {
                return (Cancel) super.setUploadType(uploadType);
              }

              @Override
              public Cancel setUploadProtocol(java.lang.String uploadProtocol) {
                return (Cancel) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the job:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /task/{task_id}/job/{job_id}`
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Required. The resource name of the job:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /task/{task_id}/job/{job_id}`
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Required. The resource name of the job:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /task/{task_id}/job/{job_id}`
               */
              public Cancel setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+/jobs/[^/]+$");
                }
                this.name = name;
                return this;
              }

              @Override
              public Cancel set(String parameterName, Object value) {
                return (Cancel) super.set(parameterName, value);
              }
            }
            /**
             * Get job resource.
             *
             * <p>Create a request for the method "jobs.get".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * @param name Required. The resource name of the job:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /tasks/{task_id}/jobs/{job_id}
             * @return the request
             */
            public Get get(java.lang.String name) throws java.io.IOException {
              Get result = new Get(name);
              initialize(result);
              return result;
            }

            public class Get
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Job> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+/jobs/[^/]+$");

              /**
               * Get job resource.
               *
               * <p>Create a request for the method "jobs.get".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Get#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Required. The resource name of the job:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /tasks/{task_id}/jobs/{job_id}
               * @since 1.13
               */
              protected Get(java.lang.String name) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Job.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+/jobs/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public Get set$Xgafv(java.lang.String $Xgafv) {
                return (Get) super.set$Xgafv($Xgafv);
              }

              @Override
              public Get setAccessToken(java.lang.String accessToken) {
                return (Get) super.setAccessToken(accessToken);
              }

              @Override
              public Get setAlt(java.lang.String alt) {
                return (Get) super.setAlt(alt);
              }

              @Override
              public Get setCallback(java.lang.String callback) {
                return (Get) super.setCallback(callback);
              }

              @Override
              public Get setFields(java.lang.String fields) {
                return (Get) super.setFields(fields);
              }

              @Override
              public Get setKey(java.lang.String key) {
                return (Get) super.setKey(key);
              }

              @Override
              public Get setOauthToken(java.lang.String oauthToken) {
                return (Get) super.setOauthToken(oauthToken);
              }

              @Override
              public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Get) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Get setQuotaUser(java.lang.String quotaUser) {
                return (Get) super.setQuotaUser(quotaUser);
              }

              @Override
              public Get setUploadType(java.lang.String uploadType) {
                return (Get) super.setUploadType(uploadType);
              }

              @Override
              public Get setUploadProtocol(java.lang.String uploadProtocol) {
                return (Get) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the job:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /tasks/{task_id}/jobs/{job_id}
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Required. The resource name of the job:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /tasks/{task_id}/jobs/{job_id}
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Required. The resource name of the job:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /tasks/{task_id}/jobs/{job_id}
               */
              public Get setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+/jobs/[^/]+$");
                }
                this.name = name;
                return this;
              }

              @Override
              public Get set(String parameterName, Object value) {
                return (Get) super.set(parameterName, value);
              }
            }
            /**
             * Lists Jobs under the given task.
             *
             * <p>Create a request for the method "jobs.list".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent environment:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/tasks/{task_id}
             * @return the request
             */
            public List list(java.lang.String parent) throws java.io.IOException {
              List result = new List(parent);
              initialize(result);
              return result;
            }

            public class List
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListJobsResponse> {

              private static final String REST_PATH = "v1/{+parent}/jobs";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");

              /**
               * Lists Jobs under the given task.
               *
               * <p>Create a request for the method "jobs.list".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link List#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent environment:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/tasks/{task_id}
               * @since 1.13
               */
              protected List(java.lang.String parent) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListJobsResponse
                        .class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public List set$Xgafv(java.lang.String $Xgafv) {
                return (List) super.set$Xgafv($Xgafv);
              }

              @Override
              public List setAccessToken(java.lang.String accessToken) {
                return (List) super.setAccessToken(accessToken);
              }

              @Override
              public List setAlt(java.lang.String alt) {
                return (List) super.setAlt(alt);
              }

              @Override
              public List setCallback(java.lang.String callback) {
                return (List) super.setCallback(callback);
              }

              @Override
              public List setFields(java.lang.String fields) {
                return (List) super.setFields(fields);
              }

              @Override
              public List setKey(java.lang.String key) {
                return (List) super.setKey(key);
              }

              @Override
              public List setOauthToken(java.lang.String oauthToken) {
                return (List) super.setOauthToken(oauthToken);
              }

              @Override
              public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (List) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public List setQuotaUser(java.lang.String quotaUser) {
                return (List) super.setQuotaUser(quotaUser);
              }

              @Override
              public List setUploadType(java.lang.String uploadType) {
                return (List) super.setUploadType(uploadType);
              }

              @Override
              public List setUploadProtocol(java.lang.String uploadProtocol) {
                return (List) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent environment:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/tasks/{task_id}
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent environment:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/tasks/{task_id}
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent environment:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/tasks/{task_id}
               */
              public List setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/tasks/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /**
               * Optional. Maximum number of jobs to return. The service may return fewer than this
               * value. If unspecified, at most 10 jobs will be returned. The maximum value is 1000;
               * values above 1000 will be coerced to 1000.
               */
              @com.google.api.client.util.Key private java.lang.Integer pageSize;

              /**
               * Optional. Maximum number of jobs to return. The service may return fewer than this
               * value. If unspecified, at most 10 jobs will be returned. The maximum value is 1000;
               * values above 1000 will be coerced to 1000.
               */
              public java.lang.Integer getPageSize() {
                return pageSize;
              }

              /**
               * Optional. Maximum number of jobs to return. The service may return fewer than this
               * value. If unspecified, at most 10 jobs will be returned. The maximum value is 1000;
               * values above 1000 will be coerced to 1000.
               */
              public List setPageSize(java.lang.Integer pageSize) {
                this.pageSize = pageSize;
                return this;
              }

              /**
               * Optional. Page token received from a previous ListJobs call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListJobs must match the call that provided the page token.
               */
              @com.google.api.client.util.Key private java.lang.String pageToken;

              /**
               * Optional. Page token received from a previous ListJobs call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListJobs must match the call that provided the page token.
               */
              public java.lang.String getPageToken() {
                return pageToken;
              }

              /**
               * Optional. Page token received from a previous ListJobs call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListJobs must match the call that provided the page token.
               */
              public List setPageToken(java.lang.String pageToken) {
                this.pageToken = pageToken;
                return this;
              }

              @Override
              public List set(String parameterName, Object value) {
                return (List) super.set(parameterName, value);
              }
            }
          }
        }
        /**
         * An accessor for creating requests from the Zones collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
         *   {@code CloudDataplex.Zones.List request = dataplex.zones().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Zones zones() {
          return new Zones();
        }

        /** The "zones" collection of methods. */
        public class Zones {

          /**
           * Creates a zone resource within a lake.
           *
           * <p>Create a request for the method "zones.create".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone}
           * @return the request
           */
          public Create create(
              java.lang.String parent,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone content)
              throws java.io.IOException {
            Create result = new Create(parent, content);
            initialize(result);
            return result;
          }

          public class Create
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+parent}/zones";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Creates a zone resource within a lake.
             *
             * <p>Create a request for the method "zones.create".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Create#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone}
             * @since 1.13
             */
            protected Create(
                java.lang.String parent,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public Create set$Xgafv(java.lang.String $Xgafv) {
              return (Create) super.set$Xgafv($Xgafv);
            }

            @Override
            public Create setAccessToken(java.lang.String accessToken) {
              return (Create) super.setAccessToken(accessToken);
            }

            @Override
            public Create setAlt(java.lang.String alt) {
              return (Create) super.setAlt(alt);
            }

            @Override
            public Create setCallback(java.lang.String callback) {
              return (Create) super.setCallback(callback);
            }

            @Override
            public Create setFields(java.lang.String fields) {
              return (Create) super.setFields(fields);
            }

            @Override
            public Create setKey(java.lang.String key) {
              return (Create) super.setKey(key);
            }

            @Override
            public Create setOauthToken(java.lang.String oauthToken) {
              return (Create) super.setOauthToken(oauthToken);
            }

            @Override
            public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Create) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Create setQuotaUser(java.lang.String quotaUser) {
              return (Create) super.setQuotaUser(quotaUser);
            }

            @Override
            public Create setUploadType(java.lang.String uploadType) {
              return (Create) super.setUploadType(uploadType);
            }

            @Override
            public Create setUploadProtocol(java.lang.String uploadProtocol) {
              return (Create) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public Create setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Create setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            /**
             * Required. Zone identifier. This ID will be used to generate names such as database
             * and dataset names when publishing metadata to Hive Metastore and BigQuery. * Must
             * contain only lowercase letters, numbers and hyphens. * Must start with a letter. *
             * Must end with a number or a letter. * Must be between 1-63 characters. * Must be
             * unique within the lake.
             */
            @com.google.api.client.util.Key private java.lang.String zoneId;

            /**
             * Required. Zone identifier. This ID will be used to generate names such as database
             * and dataset names when publishing metadata to Hive Metastore and BigQuery. * Must
             * contain only lowercase letters, numbers and hyphens. * Must start with a letter. *
             * Must end with a number or a letter. * Must be between 1-63 characters. * Must be
             * unique within the lake.
             */
            public java.lang.String getZoneId() {
              return zoneId;
            }

            /**
             * Required. Zone identifier. This ID will be used to generate names such as database
             * and dataset names when publishing metadata to Hive Metastore and BigQuery. * Must
             * contain only lowercase letters, numbers and hyphens. * Must start with a letter. *
             * Must end with a number or a letter. * Must be between 1-63 characters. * Must be
             * unique within the lake.
             */
            public Create setZoneId(java.lang.String zoneId) {
              this.zoneId = zoneId;
              return this;
            }

            @Override
            public Create set(String parameterName, Object value) {
              return (Create) super.set(parameterName, value);
            }
          }
          /**
           * Deletes a zone resource. All assets within a zone must be deleted before the zone can
           * be deleted.
           *
           * <p>Create a request for the method "zones.delete".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the zone:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
           * @return the request
           */
          public Delete delete(java.lang.String name) throws java.io.IOException {
            Delete result = new Delete(name);
            initialize(result);
            return result;
          }

          public class Delete
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Deletes a zone resource. All assets within a zone must be deleted before the zone can
             * be deleted.
             *
             * <p>Create a request for the method "zones.delete".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Delete#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}`
             * @since 1.13
             */
            protected Delete(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "DELETE",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public Delete set$Xgafv(java.lang.String $Xgafv) {
              return (Delete) super.set$Xgafv($Xgafv);
            }

            @Override
            public Delete setAccessToken(java.lang.String accessToken) {
              return (Delete) super.setAccessToken(accessToken);
            }

            @Override
            public Delete setAlt(java.lang.String alt) {
              return (Delete) super.setAlt(alt);
            }

            @Override
            public Delete setCallback(java.lang.String callback) {
              return (Delete) super.setCallback(callback);
            }

            @Override
            public Delete setFields(java.lang.String fields) {
              return (Delete) super.setFields(fields);
            }

            @Override
            public Delete setKey(java.lang.String key) {
              return (Delete) super.setKey(key);
            }

            @Override
            public Delete setOauthToken(java.lang.String oauthToken) {
              return (Delete) super.setOauthToken(oauthToken);
            }

            @Override
            public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Delete) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Delete setQuotaUser(java.lang.String quotaUser) {
              return (Delete) super.setQuotaUser(quotaUser);
            }

            @Override
            public Delete setUploadType(java.lang.String uploadType) {
              return (Delete) super.setUploadType(uploadType);
            }

            @Override
            public Delete setUploadProtocol(java.lang.String uploadProtocol) {
              return (Delete) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the zone:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the zone:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the zone:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
             */
            public Delete setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Delete set(String parameterName, Object value) {
              return (Delete) super.set(parameterName, value);
            }
          }
          /**
           * Retrieves a zone resource.
           *
           * <p>Create a request for the method "zones.get".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The resource name of the zone:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}
           * @return the request
           */
          public Get get(java.lang.String name) throws java.io.IOException {
            Get result = new Get(name);
            initialize(result);
            return result;
          }

          public class Get
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Retrieves a zone resource.
             *
             * <p>Create a request for the method "zones.get".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The resource name of the zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}
             * @since 1.13
             */
            protected Get(java.lang.String name) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public Get set$Xgafv(java.lang.String $Xgafv) {
              return (Get) super.set$Xgafv($Xgafv);
            }

            @Override
            public Get setAccessToken(java.lang.String accessToken) {
              return (Get) super.setAccessToken(accessToken);
            }

            @Override
            public Get setAlt(java.lang.String alt) {
              return (Get) super.setAlt(alt);
            }

            @Override
            public Get setCallback(java.lang.String callback) {
              return (Get) super.setCallback(callback);
            }

            @Override
            public Get setFields(java.lang.String fields) {
              return (Get) super.setFields(fields);
            }

            @Override
            public Get setKey(java.lang.String key) {
              return (Get) super.setKey(key);
            }

            @Override
            public Get setOauthToken(java.lang.String oauthToken) {
              return (Get) super.setOauthToken(oauthToken);
            }

            @Override
            public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Get) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Get setQuotaUser(java.lang.String quotaUser) {
              return (Get) super.setQuotaUser(quotaUser);
            }

            @Override
            public Get setUploadType(java.lang.String uploadType) {
              return (Get) super.setUploadType(uploadType);
            }

            @Override
            public Get setUploadProtocol(java.lang.String uploadProtocol) {
              return (Get) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the zone:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Required. The resource name of the zone:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Required. The resource name of the zone:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}
             */
            public Get setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.name = name;
              return this;
            }

            @Override
            public Get set(String parameterName, Object value) {
              return (Get) super.set(parameterName, value);
            }
          }
          /**
           * Gets the access control policy for a resource. Returns an empty policy if the resource
           * exists and does not have a policy set.
           *
           * <p>Create a request for the method "zones.getIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being requested. See the
           *     operation documentation for the appropriate value for this field.
           * @return the request
           */
          public GetIamPolicy getIamPolicy(java.lang.String resource) throws java.io.IOException {
            GetIamPolicy result = new GetIamPolicy(resource);
            initialize(result);
            return result;
          }

          public class GetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:getIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Gets the access control policy for a resource. Returns an empty policy if the
             * resource exists and does not have a policy set.
             *
             * <p>Create a request for the method "zones.getIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * GetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being requested. See
             *     the operation documentation for the appropriate value for this field.
             * @since 1.13
             */
            protected GetIamPolicy(java.lang.String resource) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public GetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (GetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public GetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (GetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public GetIamPolicy setAlt(java.lang.String alt) {
              return (GetIamPolicy) super.setAlt(alt);
            }

            @Override
            public GetIamPolicy setCallback(java.lang.String callback) {
              return (GetIamPolicy) super.setCallback(callback);
            }

            @Override
            public GetIamPolicy setFields(java.lang.String fields) {
              return (GetIamPolicy) super.setFields(fields);
            }

            @Override
            public GetIamPolicy setKey(java.lang.String key) {
              return (GetIamPolicy) super.setKey(key);
            }

            @Override
            public GetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (GetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public GetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (GetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public GetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (GetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public GetIamPolicy setUploadType(java.lang.String uploadType) {
              return (GetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public GetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (GetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being requested. See the operation
             * documentation for the appropriate value for this field.
             */
            public GetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            @com.google.api.client.util.Key("options.requestedPolicyVersion")
            private java.lang.Integer optionsRequestedPolicyVersion;

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public java.lang.Integer getOptionsRequestedPolicyVersion() {
              return optionsRequestedPolicyVersion;
            }

            /**
             * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
             * Requests specifying an invalid value will be rejected.Requests for policies with any
             * conditional bindings must specify version 3. Policies without any conditional
             * bindings may specify any valid value or leave the field unset.To learn which
             * resources support conditions in their IAM policies, see the IAM documentation
             * (https://cloud.google.com/iam/help/conditions/resource-policies).
             */
            public GetIamPolicy setOptionsRequestedPolicyVersion(
                java.lang.Integer optionsRequestedPolicyVersion) {
              this.optionsRequestedPolicyVersion = optionsRequestedPolicyVersion;
              return this;
            }

            @Override
            public GetIamPolicy set(String parameterName, Object value) {
              return (GetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Lists zone resources in a lake.
           *
           * <p>Create a request for the method "zones.list".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The resource name of the parent lake:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ListZonesResponse> {

            private static final String REST_PATH = "v1/{+parent}/zones";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");

            /**
             * Lists zone resources in a lake.
             *
             * <p>Create a request for the method "zones.list".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The resource name of the parent lake:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  CloudDataplex.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ListZonesResponse
                      .class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
            }

            @Override
            public com.google.api.client.http.HttpResponse executeUsingHead()
                throws java.io.IOException {
              return super.executeUsingHead();
            }

            @Override
            public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                throws java.io.IOException {
              return super.buildHttpRequestUsingHead();
            }

            @Override
            public List set$Xgafv(java.lang.String $Xgafv) {
              return (List) super.set$Xgafv($Xgafv);
            }

            @Override
            public List setAccessToken(java.lang.String accessToken) {
              return (List) super.setAccessToken(accessToken);
            }

            @Override
            public List setAlt(java.lang.String alt) {
              return (List) super.setAlt(alt);
            }

            @Override
            public List setCallback(java.lang.String callback) {
              return (List) super.setCallback(callback);
            }

            @Override
            public List setFields(java.lang.String fields) {
              return (List) super.setFields(fields);
            }

            @Override
            public List setKey(java.lang.String key) {
              return (List) super.setKey(key);
            }

            @Override
            public List setOauthToken(java.lang.String oauthToken) {
              return (List) super.setOauthToken(oauthToken);
            }

            @Override
            public List setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (List) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public List setQuotaUser(java.lang.String quotaUser) {
              return (List) super.setQuotaUser(quotaUser);
            }

            @Override
            public List setUploadType(java.lang.String uploadType) {
              return (List) super.setUploadType(uploadType);
            }

            @Override
            public List setUploadProtocol(java.lang.String uploadProtocol) {
              return (List) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            @com.google.api.client.util.Key private java.lang.String parent;

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public java.lang.String getParent() {
              return parent;
            }

            /**
             * Required. The resource name of the parent lake:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /** Optional. Filter request. */
            @com.google.api.client.util.Key private java.lang.String filter;

            /** Optional. Filter request. */
            public java.lang.String getFilter() {
              return filter;
            }

            /** Optional. Filter request. */
            public List setFilter(java.lang.String filter) {
              this.filter = filter;
              return this;
            }

            /** Optional. Order by fields for the result. */
            @com.google.api.client.util.Key private java.lang.String orderBy;

            /** Optional. Order by fields for the result. */
            public java.lang.String getOrderBy() {
              return orderBy;
            }

            /** Optional. Order by fields for the result. */
            public List setOrderBy(java.lang.String orderBy) {
              this.orderBy = orderBy;
              return this;
            }

            /**
             * Optional. Maximum number of zones to return. The service may return fewer than this
             * value. If unspecified, at most 10 zones will be returned. The maximum value is 1000;
             * values above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Optional. Maximum number of zones to return. The service may return fewer than this
             * value. If unspecified, at most 10 zones will be returned. The maximum value is 1000;
             * values above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Optional. Maximum number of zones to return. The service may return fewer than this
             * value. If unspecified, at most 10 zones will be returned. The maximum value is 1000;
             * values above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Optional. Page token received from a previous ListZones call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListZones must match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Optional. Page token received from a previous ListZones call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListZones must match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Optional. Page token received from a previous ListZones call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * ListZones must match the call that provided the page token.
             */
            public List setPageToken(java.lang.String pageToken) {
              this.pageToken = pageToken;
              return this;
            }

            @Override
            public List set(String parameterName, Object value) {
              return (List) super.set(parameterName, value);
            }
          }
          /**
           * Updates a zone resource.
           *
           * <p>Create a request for the method "zones.patch".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * @param name Output only. The relative resource name of the zone, of the form:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone}
           * @return the request
           */
          public Patch patch(
              java.lang.String name,
              com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone content)
              throws java.io.IOException {
            Patch result = new Patch(name, content);
            initialize(result);
            return result;
          }

          public class Patch
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Updates a zone resource.
             *
             * <p>Create a request for the method "zones.patch".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Output only. The relative resource name of the zone, of the form:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone}
             * @since 1.13
             */
            protected Patch(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Zone content) {
              super(
                  CloudDataplex.this,
                  "PATCH",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public Patch set$Xgafv(java.lang.String $Xgafv) {
              return (Patch) super.set$Xgafv($Xgafv);
            }

            @Override
            public Patch setAccessToken(java.lang.String accessToken) {
              return (Patch) super.setAccessToken(accessToken);
            }

            @Override
            public Patch setAlt(java.lang.String alt) {
              return (Patch) super.setAlt(alt);
            }

            @Override
            public Patch setCallback(java.lang.String callback) {
              return (Patch) super.setCallback(callback);
            }

            @Override
            public Patch setFields(java.lang.String fields) {
              return (Patch) super.setFields(fields);
            }

            @Override
            public Patch setKey(java.lang.String key) {
              return (Patch) super.setKey(key);
            }

            @Override
            public Patch setOauthToken(java.lang.String oauthToken) {
              return (Patch) super.setOauthToken(oauthToken);
            }

            @Override
            public Patch setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Patch) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Patch setQuotaUser(java.lang.String quotaUser) {
              return (Patch) super.setQuotaUser(quotaUser);
            }

            @Override
            public Patch setUploadType(java.lang.String uploadType) {
              return (Patch) super.setUploadType(uploadType);
            }

            @Override
            public Patch setUploadProtocol(java.lang.String uploadProtocol) {
              return (Patch) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Output only. The relative resource name of the zone, of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             */
            @com.google.api.client.util.Key private java.lang.String name;

            /**
             * Output only. The relative resource name of the zone, of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             */
            public java.lang.String getName() {
              return name;
            }

            /**
             * Output only. The relative resource name of the zone, of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             */
            public Patch setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.name = name;
              return this;
            }

            /** Required. Mask of fields to update. */
            @com.google.api.client.util.Key private String updateMask;

            /** Required. Mask of fields to update. */
            public String getUpdateMask() {
              return updateMask;
            }

            /** Required. Mask of fields to update. */
            public Patch setUpdateMask(String updateMask) {
              this.updateMask = updateMask;
              return this;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public java.lang.Boolean getValidateOnly() {
              return validateOnly;
            }

            /**
             * Optional. Only validate the request, but do not perform mutations. The default is
             * false.
             */
            public Patch setValidateOnly(java.lang.Boolean validateOnly) {
              this.validateOnly = validateOnly;
              return this;
            }

            @Override
            public Patch set(String parameterName, Object value) {
              return (Patch) super.set(parameterName, value);
            }
          }
          /**
           * Marks actions associated with a zone as resolved.
           *
           * <p>Create a request for the method "zones.resolveZoneActions".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link ResolveZoneActions#execute()} method to invoke the
           * remote operation.
           *
           * @param zone Required. The name of the zone for which actions are being resolved of the
           *     form:
           *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveZoneActionsRequest}
           * @return the request
           */
          public ResolveZoneActions resolveZoneActions(
              java.lang.String zone,
              com.google.api.services.dataplex.v1.model
                      .GoogleCloudDataplexV1ResolveZoneActionsRequest
                  content)
              throws java.io.IOException {
            ResolveZoneActions result = new ResolveZoneActions(zone, content);
            initialize(result);
            return result;
          }

          public class ResolveZoneActions
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

            private static final String REST_PATH = "v1/{+zone}:resolveZoneActions";

            private final java.util.regex.Pattern ZONE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Marks actions associated with a zone as resolved.
             *
             * <p>Create a request for the method "zones.resolveZoneActions".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link ResolveZoneActions#execute()} method to
             * invoke the remote operation.
             *
             * <p>{@link ResolveZoneActions#initialize(com.google.api.client.googleapis.services.A
             * bstractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param zone Required. The name of the zone for which actions are being resolved of
             *     the form:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveZoneActionsRequest}
             * @since 1.13
             */
            protected ResolveZoneActions(
                java.lang.String zone,
                com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ResolveZoneActionsRequest
                    content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
              this.zone =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      zone, "Required parameter zone must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    ZONE_PATTERN.matcher(zone).matches(),
                    "Parameter zone must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public ResolveZoneActions set$Xgafv(java.lang.String $Xgafv) {
              return (ResolveZoneActions) super.set$Xgafv($Xgafv);
            }

            @Override
            public ResolveZoneActions setAccessToken(java.lang.String accessToken) {
              return (ResolveZoneActions) super.setAccessToken(accessToken);
            }

            @Override
            public ResolveZoneActions setAlt(java.lang.String alt) {
              return (ResolveZoneActions) super.setAlt(alt);
            }

            @Override
            public ResolveZoneActions setCallback(java.lang.String callback) {
              return (ResolveZoneActions) super.setCallback(callback);
            }

            @Override
            public ResolveZoneActions setFields(java.lang.String fields) {
              return (ResolveZoneActions) super.setFields(fields);
            }

            @Override
            public ResolveZoneActions setKey(java.lang.String key) {
              return (ResolveZoneActions) super.setKey(key);
            }

            @Override
            public ResolveZoneActions setOauthToken(java.lang.String oauthToken) {
              return (ResolveZoneActions) super.setOauthToken(oauthToken);
            }

            @Override
            public ResolveZoneActions setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (ResolveZoneActions) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public ResolveZoneActions setQuotaUser(java.lang.String quotaUser) {
              return (ResolveZoneActions) super.setQuotaUser(quotaUser);
            }

            @Override
            public ResolveZoneActions setUploadType(java.lang.String uploadType) {
              return (ResolveZoneActions) super.setUploadType(uploadType);
            }

            @Override
            public ResolveZoneActions setUploadProtocol(java.lang.String uploadProtocol) {
              return (ResolveZoneActions) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * Required. The name of the zone for which actions are being resolved of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             */
            @com.google.api.client.util.Key private java.lang.String zone;

            /**
             * Required. The name of the zone for which actions are being resolved of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             */
            public java.lang.String getZone() {
              return zone;
            }

            /**
             * Required. The name of the zone for which actions are being resolved of the form:
             * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             */
            public ResolveZoneActions setZone(java.lang.String zone) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    ZONE_PATTERN.matcher(zone).matches(),
                    "Parameter zone must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.zone = zone;
              return this;
            }

            @Override
            public ResolveZoneActions set(String parameterName, Object value) {
              return (ResolveZoneActions) super.set(parameterName, value);
            }
          }
          /**
           * Sets the access control policy on the specified resource. Replaces any existing
           * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
           *
           * <p>Create a request for the method "zones.setIamPolicy".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy is being specified. See the
           *     operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
           * @return the request
           */
          public SetIamPolicy setIamPolicy(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content)
              throws java.io.IOException {
            SetIamPolicy result = new SetIamPolicy(resource, content);
            initialize(result);
            return result;
          }

          public class SetIamPolicy
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

            private static final String REST_PATH = "v1/{+resource}:setIamPolicy";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Sets the access control policy on the specified resource. Replaces any existing
             * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
             *
             * <p>Create a request for the method "zones.setIamPolicy".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * SetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param resource REQUIRED: The resource for which the policy is being specified. See
             *     the operation documentation for the appropriate value for this field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
             * @since 1.13
             */
            protected SetIamPolicy(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public SetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
              return (SetIamPolicy) super.set$Xgafv($Xgafv);
            }

            @Override
            public SetIamPolicy setAccessToken(java.lang.String accessToken) {
              return (SetIamPolicy) super.setAccessToken(accessToken);
            }

            @Override
            public SetIamPolicy setAlt(java.lang.String alt) {
              return (SetIamPolicy) super.setAlt(alt);
            }

            @Override
            public SetIamPolicy setCallback(java.lang.String callback) {
              return (SetIamPolicy) super.setCallback(callback);
            }

            @Override
            public SetIamPolicy setFields(java.lang.String fields) {
              return (SetIamPolicy) super.setFields(fields);
            }

            @Override
            public SetIamPolicy setKey(java.lang.String key) {
              return (SetIamPolicy) super.setKey(key);
            }

            @Override
            public SetIamPolicy setOauthToken(java.lang.String oauthToken) {
              return (SetIamPolicy) super.setOauthToken(oauthToken);
            }

            @Override
            public SetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (SetIamPolicy) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public SetIamPolicy setQuotaUser(java.lang.String quotaUser) {
              return (SetIamPolicy) super.setQuotaUser(quotaUser);
            }

            @Override
            public SetIamPolicy setUploadType(java.lang.String uploadType) {
              return (SetIamPolicy) super.setUploadType(uploadType);
            }

            @Override
            public SetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
              return (SetIamPolicy) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy is being specified. See the operation
             * documentation for the appropriate value for this field.
             */
            public SetIamPolicy setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public SetIamPolicy set(String parameterName, Object value) {
              return (SetIamPolicy) super.set(parameterName, value);
            }
          }
          /**
           * Returns permissions that a caller has on the specified resource. If the resource does
           * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
           * This operation is designed to be used for building permission-aware UIs and
           * command-line tools, not for authorization checking. This operation may "fail open"
           * without warning.
           *
           * <p>Create a request for the method "zones.testIamPermissions".
           *
           * <p>This request holds the parameters needed by the dataplex server. After setting any
           * optional parameters, call the {@link TestIamPermissions#execute()} method to invoke the
           * remote operation.
           *
           * @param resource REQUIRED: The resource for which the policy detail is being requested.
           *     See the operation documentation for the appropriate value for this field.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
           * @return the request
           */
          public TestIamPermissions testIamPermissions(
              java.lang.String resource,
              com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                  content)
              throws java.io.IOException {
            TestIamPermissions result = new TestIamPermissions(resource, content);
            initialize(result);
            return result;
          }

          public class TestIamPermissions
              extends CloudDataplexRequest<
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse> {

            private static final String REST_PATH = "v1/{+resource}:testIamPermissions";

            private final java.util.regex.Pattern RESOURCE_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

            /**
             * Returns permissions that a caller has on the specified resource. If the resource does
             * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
             * This operation is designed to be used for building permission-aware UIs and
             * command-line tools, not for authorization checking. This operation may "fail open"
             * without warning.
             *
             * <p>Create a request for the method "zones.testIamPermissions".
             *
             * <p>This request holds the parameters needed by the the dataplex server. After setting
             * any optional parameters, call the {@link TestIamPermissions#execute()} method to
             * invoke the remote operation.
             *
             * <p>{@link TestIamPermissions#initialize(com.google.api.client.googleapis.services.A
             * bstractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param resource REQUIRED: The resource for which the policy detail is being
             *     requested. See the operation documentation for the appropriate value for this
             *     field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
             * @since 1.13
             */
            protected TestIamPermissions(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                    content) {
              super(
                  CloudDataplex.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse
                      .class);
              this.resource =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      resource, "Required parameter resource must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
            }

            @Override
            public TestIamPermissions set$Xgafv(java.lang.String $Xgafv) {
              return (TestIamPermissions) super.set$Xgafv($Xgafv);
            }

            @Override
            public TestIamPermissions setAccessToken(java.lang.String accessToken) {
              return (TestIamPermissions) super.setAccessToken(accessToken);
            }

            @Override
            public TestIamPermissions setAlt(java.lang.String alt) {
              return (TestIamPermissions) super.setAlt(alt);
            }

            @Override
            public TestIamPermissions setCallback(java.lang.String callback) {
              return (TestIamPermissions) super.setCallback(callback);
            }

            @Override
            public TestIamPermissions setFields(java.lang.String fields) {
              return (TestIamPermissions) super.setFields(fields);
            }

            @Override
            public TestIamPermissions setKey(java.lang.String key) {
              return (TestIamPermissions) super.setKey(key);
            }

            @Override
            public TestIamPermissions setOauthToken(java.lang.String oauthToken) {
              return (TestIamPermissions) super.setOauthToken(oauthToken);
            }

            @Override
            public TestIamPermissions setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (TestIamPermissions) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public TestIamPermissions setQuotaUser(java.lang.String quotaUser) {
              return (TestIamPermissions) super.setQuotaUser(quotaUser);
            }

            @Override
            public TestIamPermissions setUploadType(java.lang.String uploadType) {
              return (TestIamPermissions) super.setUploadType(uploadType);
            }

            @Override
            public TestIamPermissions setUploadProtocol(java.lang.String uploadProtocol) {
              return (TestIamPermissions) super.setUploadProtocol(uploadProtocol);
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            @com.google.api.client.util.Key private java.lang.String resource;

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public java.lang.String getResource() {
              return resource;
            }

            /**
             * REQUIRED: The resource for which the policy detail is being requested. See the
             * operation documentation for the appropriate value for this field.
             */
            public TestIamPermissions setResource(java.lang.String resource) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    RESOURCE_PATTERN.matcher(resource).matches(),
                    "Parameter resource must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
              }
              this.resource = resource;
              return this;
            }

            @Override
            public TestIamPermissions set(String parameterName, Object value) {
              return (TestIamPermissions) super.set(parameterName, value);
            }
          }

          /**
           * An accessor for creating requests from the Actions collection.
           *
           * <p>The typical use is:
           *
           * <pre>
           *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
           *   {@code CloudDataplex.Actions.List request = dataplex.actions().list(parameters ...)}
           * </pre>
           *
           * @return the resource collection
           */
          public Actions actions() {
            return new Actions();
          }

          /** The "actions" collection of methods. */
          public class Actions {

            /**
             * Lists action resources in a zone.
             *
             * <p>Create a request for the method "actions.list".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
             * @return the request
             */
            public List list(java.lang.String parent) throws java.io.IOException {
              List result = new List(parent);
              initialize(result);
              return result;
            }

            public class List
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListActionsResponse> {

              private static final String REST_PATH = "v1/{+parent}/actions";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

              /**
               * Lists action resources in a zone.
               *
               * <p>Create a request for the method "actions.list".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link List#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent zone:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
               * @since 1.13
               */
              protected List(java.lang.String parent) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListActionsResponse.class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public List set$Xgafv(java.lang.String $Xgafv) {
                return (List) super.set$Xgafv($Xgafv);
              }

              @Override
              public List setAccessToken(java.lang.String accessToken) {
                return (List) super.setAccessToken(accessToken);
              }

              @Override
              public List setAlt(java.lang.String alt) {
                return (List) super.setAlt(alt);
              }

              @Override
              public List setCallback(java.lang.String callback) {
                return (List) super.setCallback(callback);
              }

              @Override
              public List setFields(java.lang.String fields) {
                return (List) super.setFields(fields);
              }

              @Override
              public List setKey(java.lang.String key) {
                return (List) super.setKey(key);
              }

              @Override
              public List setOauthToken(java.lang.String oauthToken) {
                return (List) super.setOauthToken(oauthToken);
              }

              @Override
              public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (List) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public List setQuotaUser(java.lang.String quotaUser) {
                return (List) super.setQuotaUser(quotaUser);
              }

              @Override
              public List setUploadType(java.lang.String uploadType) {
                return (List) super.setUploadType(uploadType);
              }

              @Override
              public List setUploadProtocol(java.lang.String uploadProtocol) {
                return (List) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}
               */
              public List setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /**
               * Optional. Maximum number of actions to return. The service may return fewer than
               * this value. If unspecified, at most 10 actions will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              @com.google.api.client.util.Key private java.lang.Integer pageSize;

              /**
               * Optional. Maximum number of actions to return. The service may return fewer than
               * this value. If unspecified, at most 10 actions will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              public java.lang.Integer getPageSize() {
                return pageSize;
              }

              /**
               * Optional. Maximum number of actions to return. The service may return fewer than
               * this value. If unspecified, at most 10 actions will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              public List setPageSize(java.lang.Integer pageSize) {
                this.pageSize = pageSize;
                return this;
              }

              /**
               * Optional. Page token received from a previous ListZoneActions call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListZoneActions must match the call that provided the page token.
               */
              @com.google.api.client.util.Key private java.lang.String pageToken;

              /**
               * Optional. Page token received from a previous ListZoneActions call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListZoneActions must match the call that provided the page token.
               */
              public java.lang.String getPageToken() {
                return pageToken;
              }

              /**
               * Optional. Page token received from a previous ListZoneActions call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListZoneActions must match the call that provided the page token.
               */
              public List setPageToken(java.lang.String pageToken) {
                this.pageToken = pageToken;
                return this;
              }

              @Override
              public List set(String parameterName, Object value) {
                return (List) super.set(parameterName, value);
              }
            }
          }
          /**
           * An accessor for creating requests from the Assets collection.
           *
           * <p>The typical use is:
           *
           * <pre>
           *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
           *   {@code CloudDataplex.Assets.List request = dataplex.assets().list(parameters ...)}
           * </pre>
           *
           * @return the resource collection
           */
          public Assets assets() {
            return new Assets();
          }

          /** The "assets" collection of methods. */
          public class Assets {

            /**
             * Creates an asset resource.
             *
             * <p>Create a request for the method "assets.create".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Create#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}`
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset}
             * @return the request
             */
            public Create create(
                java.lang.String parent,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset content)
                throws java.io.IOException {
              Create result = new Create(parent, content);
              initialize(result);
              return result;
            }

            public class Create
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

              private static final String REST_PATH = "v1/{+parent}/assets";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

              /**
               * Creates an asset resource.
               *
               * <p>Create a request for the method "assets.create".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Create#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent zone:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}`
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset}
               * @since 1.13
               */
              protected Create(
                  java.lang.String parent,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset content) {
                super(
                    CloudDataplex.this,
                    "POST",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
              }

              @Override
              public Create set$Xgafv(java.lang.String $Xgafv) {
                return (Create) super.set$Xgafv($Xgafv);
              }

              @Override
              public Create setAccessToken(java.lang.String accessToken) {
                return (Create) super.setAccessToken(accessToken);
              }

              @Override
              public Create setAlt(java.lang.String alt) {
                return (Create) super.setAlt(alt);
              }

              @Override
              public Create setCallback(java.lang.String callback) {
                return (Create) super.setCallback(callback);
              }

              @Override
              public Create setFields(java.lang.String fields) {
                return (Create) super.setFields(fields);
              }

              @Override
              public Create setKey(java.lang.String key) {
                return (Create) super.setKey(key);
              }

              @Override
              public Create setOauthToken(java.lang.String oauthToken) {
                return (Create) super.setOauthToken(oauthToken);
              }

              @Override
              public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Create) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Create setQuotaUser(java.lang.String quotaUser) {
                return (Create) super.setQuotaUser(quotaUser);
              }

              @Override
              public Create setUploadType(java.lang.String uploadType) {
                return (Create) super.setUploadType(uploadType);
              }

              @Override
              public Create setUploadProtocol(java.lang.String uploadProtocol) {
                return (Create) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public Create setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /**
               * Required. Asset identifier. This ID will be used to generate names such as table
               * names when publishing metadata to Hive Metastore and BigQuery. * Must contain only
               * lowercase letters, numbers and hyphens. * Must start with a letter. * Must end with
               * a number or a letter. * Must be between 1-63 characters. * Must be unique within
               * the zone.
               */
              @com.google.api.client.util.Key private java.lang.String assetId;

              /**
               * Required. Asset identifier. This ID will be used to generate names such as table
               * names when publishing metadata to Hive Metastore and BigQuery. * Must contain only
               * lowercase letters, numbers and hyphens. * Must start with a letter. * Must end with
               * a number or a letter. * Must be between 1-63 characters. * Must be unique within
               * the zone.
               */
              public java.lang.String getAssetId() {
                return assetId;
              }

              /**
               * Required. Asset identifier. This ID will be used to generate names such as table
               * names when publishing metadata to Hive Metastore and BigQuery. * Must contain only
               * lowercase letters, numbers and hyphens. * Must start with a letter. * Must end with
               * a number or a letter. * Must be between 1-63 characters. * Must be unique within
               * the zone.
               */
              public Create setAssetId(java.lang.String assetId) {
                this.assetId = assetId;
                return this;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public java.lang.Boolean getValidateOnly() {
                return validateOnly;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public Create setValidateOnly(java.lang.Boolean validateOnly) {
                this.validateOnly = validateOnly;
                return this;
              }

              @Override
              public Create set(String parameterName, Object value) {
                return (Create) super.set(parameterName, value);
              }
            }
            /**
             * Deletes an asset resource. The referenced storage resource is detached (default) or
             * deleted based on the associated Lifecycle policy.
             *
             * <p>Create a request for the method "assets.delete".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Delete#execute()} method to invoke the remote
             * operation.
             *
             * @param name Required. The resource name of the asset:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}/assets/{asset_id}
             * @return the request
             */
            public Delete delete(java.lang.String name) throws java.io.IOException {
              Delete result = new Delete(name);
              initialize(result);
              return result;
            }

            public class Delete
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Deletes an asset resource. The referenced storage resource is detached (default) or
               * deleted based on the associated Lifecycle policy.
               *
               * <p>Create a request for the method "assets.delete".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Delete#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Required. The resource name of the asset:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/assets/{asset_id}
               * @since 1.13
               */
              protected Delete(java.lang.String name) {
                super(
                    CloudDataplex.this,
                    "DELETE",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public Delete set$Xgafv(java.lang.String $Xgafv) {
                return (Delete) super.set$Xgafv($Xgafv);
              }

              @Override
              public Delete setAccessToken(java.lang.String accessToken) {
                return (Delete) super.setAccessToken(accessToken);
              }

              @Override
              public Delete setAlt(java.lang.String alt) {
                return (Delete) super.setAlt(alt);
              }

              @Override
              public Delete setCallback(java.lang.String callback) {
                return (Delete) super.setCallback(callback);
              }

              @Override
              public Delete setFields(java.lang.String fields) {
                return (Delete) super.setFields(fields);
              }

              @Override
              public Delete setKey(java.lang.String key) {
                return (Delete) super.setKey(key);
              }

              @Override
              public Delete setOauthToken(java.lang.String oauthToken) {
                return (Delete) super.setOauthToken(oauthToken);
              }

              @Override
              public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Delete) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Delete setQuotaUser(java.lang.String quotaUser) {
                return (Delete) super.setQuotaUser(quotaUser);
              }

              @Override
              public Delete setUploadType(java.lang.String uploadType) {
                return (Delete) super.setUploadType(uploadType);
              }

              @Override
              public Delete setUploadProtocol(java.lang.String uploadProtocol) {
                return (Delete) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the asset:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/assets/{asset_id}
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Required. The resource name of the asset:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/assets/{asset_id}
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Required. The resource name of the asset:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/assets/{asset_id}
               */
              public Delete setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.name = name;
                return this;
              }

              @Override
              public Delete set(String parameterName, Object value) {
                return (Delete) super.set(parameterName, value);
              }
            }
            /**
             * Retrieves an asset resource.
             *
             * <p>Create a request for the method "assets.get".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * @param name Required. The resource name of the asset:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}/assets/{asset_id}
             * @return the request
             */
            public Get get(java.lang.String name) throws java.io.IOException {
              Get result = new Get(name);
              initialize(result);
              return result;
            }

            public class Get
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Retrieves an asset resource.
               *
               * <p>Create a request for the method "assets.get".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Get#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Required. The resource name of the asset:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/assets/{asset_id}
               * @since 1.13
               */
              protected Get(java.lang.String name) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public Get set$Xgafv(java.lang.String $Xgafv) {
                return (Get) super.set$Xgafv($Xgafv);
              }

              @Override
              public Get setAccessToken(java.lang.String accessToken) {
                return (Get) super.setAccessToken(accessToken);
              }

              @Override
              public Get setAlt(java.lang.String alt) {
                return (Get) super.setAlt(alt);
              }

              @Override
              public Get setCallback(java.lang.String callback) {
                return (Get) super.setCallback(callback);
              }

              @Override
              public Get setFields(java.lang.String fields) {
                return (Get) super.setFields(fields);
              }

              @Override
              public Get setKey(java.lang.String key) {
                return (Get) super.setKey(key);
              }

              @Override
              public Get setOauthToken(java.lang.String oauthToken) {
                return (Get) super.setOauthToken(oauthToken);
              }

              @Override
              public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Get) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Get setQuotaUser(java.lang.String quotaUser) {
                return (Get) super.setQuotaUser(quotaUser);
              }

              @Override
              public Get setUploadType(java.lang.String uploadType) {
                return (Get) super.setUploadType(uploadType);
              }

              @Override
              public Get setUploadProtocol(java.lang.String uploadProtocol) {
                return (Get) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the asset:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/assets/{asset_id}
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Required. The resource name of the asset:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/assets/{asset_id}
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Required. The resource name of the asset:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/assets/{asset_id}
               */
              public Get setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.name = name;
                return this;
              }

              @Override
              public Get set(String parameterName, Object value) {
                return (Get) super.set(parameterName, value);
              }
            }
            /**
             * Gets the access control policy for a resource. Returns an empty policy if the
             * resource exists and does not have a policy set.
             *
             * <p>Create a request for the method "assets.getIamPolicy".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link GetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * @param resource REQUIRED: The resource for which the policy is being requested. See
             *     the operation documentation for the appropriate value for this field.
             * @return the request
             */
            public GetIamPolicy getIamPolicy(java.lang.String resource) throws java.io.IOException {
              GetIamPolicy result = new GetIamPolicy(resource);
              initialize(result);
              return result;
            }

            public class GetIamPolicy
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

              private static final String REST_PATH = "v1/{+resource}:getIamPolicy";

              private final java.util.regex.Pattern RESOURCE_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Gets the access control policy for a resource. Returns an empty policy if the
               * resource exists and does not have a policy set.
               *
               * <p>Create a request for the method "assets.getIamPolicy".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link GetIamPolicy#execute()} method to
               * invoke the remote operation.
               *
               * <p>{@link
               * GetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param resource REQUIRED: The resource for which the policy is being requested. See
               *     the operation documentation for the appropriate value for this field.
               * @since 1.13
               */
              protected GetIamPolicy(java.lang.String resource) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
                this.resource =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        resource, "Required parameter resource must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      RESOURCE_PATTERN.matcher(resource).matches(),
                      "Parameter resource must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public GetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
                return (GetIamPolicy) super.set$Xgafv($Xgafv);
              }

              @Override
              public GetIamPolicy setAccessToken(java.lang.String accessToken) {
                return (GetIamPolicy) super.setAccessToken(accessToken);
              }

              @Override
              public GetIamPolicy setAlt(java.lang.String alt) {
                return (GetIamPolicy) super.setAlt(alt);
              }

              @Override
              public GetIamPolicy setCallback(java.lang.String callback) {
                return (GetIamPolicy) super.setCallback(callback);
              }

              @Override
              public GetIamPolicy setFields(java.lang.String fields) {
                return (GetIamPolicy) super.setFields(fields);
              }

              @Override
              public GetIamPolicy setKey(java.lang.String key) {
                return (GetIamPolicy) super.setKey(key);
              }

              @Override
              public GetIamPolicy setOauthToken(java.lang.String oauthToken) {
                return (GetIamPolicy) super.setOauthToken(oauthToken);
              }

              @Override
              public GetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (GetIamPolicy) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public GetIamPolicy setQuotaUser(java.lang.String quotaUser) {
                return (GetIamPolicy) super.setQuotaUser(quotaUser);
              }

              @Override
              public GetIamPolicy setUploadType(java.lang.String uploadType) {
                return (GetIamPolicy) super.setUploadType(uploadType);
              }

              @Override
              public GetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
                return (GetIamPolicy) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * REQUIRED: The resource for which the policy is being requested. See the operation
               * documentation for the appropriate value for this field.
               */
              @com.google.api.client.util.Key private java.lang.String resource;

              /**
               * REQUIRED: The resource for which the policy is being requested. See the operation
               * documentation for the appropriate value for this field.
               */
              public java.lang.String getResource() {
                return resource;
              }

              /**
               * REQUIRED: The resource for which the policy is being requested. See the operation
               * documentation for the appropriate value for this field.
               */
              public GetIamPolicy setResource(java.lang.String resource) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      RESOURCE_PATTERN.matcher(resource).matches(),
                      "Parameter resource must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.resource = resource;
                return this;
              }

              /**
               * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
               * Requests specifying an invalid value will be rejected.Requests for policies with
               * any conditional bindings must specify version 3. Policies without any conditional
               * bindings may specify any valid value or leave the field unset.To learn which
               * resources support conditions in their IAM policies, see the IAM documentation
               * (https://cloud.google.com/iam/help/conditions/resource-policies).
               */
              @com.google.api.client.util.Key("options.requestedPolicyVersion")
              private java.lang.Integer optionsRequestedPolicyVersion;

              /**
               * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
               * Requests specifying an invalid value will be rejected.Requests for policies with
               * any conditional bindings must specify version 3. Policies without any conditional
               * bindings may specify any valid value or leave the field unset.To learn which
               * resources support conditions in their IAM policies, see the IAM documentation
               * (https://cloud.google.com/iam/help/conditions/resource-policies).
               */
              public java.lang.Integer getOptionsRequestedPolicyVersion() {
                return optionsRequestedPolicyVersion;
              }

              /**
               * Optional. The policy format version to be returned.Valid values are 0, 1, and 3.
               * Requests specifying an invalid value will be rejected.Requests for policies with
               * any conditional bindings must specify version 3. Policies without any conditional
               * bindings may specify any valid value or leave the field unset.To learn which
               * resources support conditions in their IAM policies, see the IAM documentation
               * (https://cloud.google.com/iam/help/conditions/resource-policies).
               */
              public GetIamPolicy setOptionsRequestedPolicyVersion(
                  java.lang.Integer optionsRequestedPolicyVersion) {
                this.optionsRequestedPolicyVersion = optionsRequestedPolicyVersion;
                return this;
              }

              @Override
              public GetIamPolicy set(String parameterName, Object value) {
                return (GetIamPolicy) super.set(parameterName, value);
              }
            }
            /**
             * Lists asset resources in a zone.
             *
             * <p>Create a request for the method "assets.list".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}`
             * @return the request
             */
            public List list(java.lang.String parent) throws java.io.IOException {
              List result = new List(parent);
              initialize(result);
              return result;
            }

            public class List
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListAssetsResponse> {

              private static final String REST_PATH = "v1/{+parent}/assets";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

              /**
               * Lists asset resources in a zone.
               *
               * <p>Create a request for the method "assets.list".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link List#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent zone:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}`
               * @since 1.13
               */
              protected List(java.lang.String parent) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListAssetsResponse.class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public List set$Xgafv(java.lang.String $Xgafv) {
                return (List) super.set$Xgafv($Xgafv);
              }

              @Override
              public List setAccessToken(java.lang.String accessToken) {
                return (List) super.setAccessToken(accessToken);
              }

              @Override
              public List setAlt(java.lang.String alt) {
                return (List) super.setAlt(alt);
              }

              @Override
              public List setCallback(java.lang.String callback) {
                return (List) super.setCallback(callback);
              }

              @Override
              public List setFields(java.lang.String fields) {
                return (List) super.setFields(fields);
              }

              @Override
              public List setKey(java.lang.String key) {
                return (List) super.setKey(key);
              }

              @Override
              public List setOauthToken(java.lang.String oauthToken) {
                return (List) super.setOauthToken(oauthToken);
              }

              @Override
              public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (List) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public List setQuotaUser(java.lang.String quotaUser) {
                return (List) super.setQuotaUser(quotaUser);
              }

              @Override
              public List setUploadType(java.lang.String uploadType) {
                return (List) super.setUploadType(uploadType);
              }

              @Override
              public List setUploadProtocol(java.lang.String uploadProtocol) {
                return (List) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public List setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /** Optional. Filter request. */
              @com.google.api.client.util.Key private java.lang.String filter;

              /** Optional. Filter request. */
              public java.lang.String getFilter() {
                return filter;
              }

              /** Optional. Filter request. */
              public List setFilter(java.lang.String filter) {
                this.filter = filter;
                return this;
              }

              /** Optional. Order by fields for the result. */
              @com.google.api.client.util.Key private java.lang.String orderBy;

              /** Optional. Order by fields for the result. */
              public java.lang.String getOrderBy() {
                return orderBy;
              }

              /** Optional. Order by fields for the result. */
              public List setOrderBy(java.lang.String orderBy) {
                this.orderBy = orderBy;
                return this;
              }

              /**
               * Optional. Maximum number of asset to return. The service may return fewer than this
               * value. If unspecified, at most 10 assets will be returned. The maximum value is
               * 1000; values above 1000 will be coerced to 1000.
               */
              @com.google.api.client.util.Key private java.lang.Integer pageSize;

              /**
               * Optional. Maximum number of asset to return. The service may return fewer than this
               * value. If unspecified, at most 10 assets will be returned. The maximum value is
               * 1000; values above 1000 will be coerced to 1000.
               */
              public java.lang.Integer getPageSize() {
                return pageSize;
              }

              /**
               * Optional. Maximum number of asset to return. The service may return fewer than this
               * value. If unspecified, at most 10 assets will be returned. The maximum value is
               * 1000; values above 1000 will be coerced to 1000.
               */
              public List setPageSize(java.lang.Integer pageSize) {
                this.pageSize = pageSize;
                return this;
              }

              /**
               * Optional. Page token received from a previous ListAssets call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListAssets must match the call that provided the page token.
               */
              @com.google.api.client.util.Key private java.lang.String pageToken;

              /**
               * Optional. Page token received from a previous ListAssets call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListAssets must match the call that provided the page token.
               */
              public java.lang.String getPageToken() {
                return pageToken;
              }

              /**
               * Optional. Page token received from a previous ListAssets call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListAssets must match the call that provided the page token.
               */
              public List setPageToken(java.lang.String pageToken) {
                this.pageToken = pageToken;
                return this;
              }

              @Override
              public List set(String parameterName, Object value) {
                return (List) super.set(parameterName, value);
              }
            }
            /**
             * Updates an asset resource.
             *
             * <p>Create a request for the method "assets.patch".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Patch#execute()} method to invoke the remote
             * operation.
             *
             * @param name Output only. The relative resource name of the asset, of the form:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{
             *     asset_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset}
             * @return the request
             */
            public Patch patch(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset content)
                throws java.io.IOException {
              Patch result = new Patch(name, content);
              initialize(result);
              return result;
            }

            public class Patch
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Updates an asset resource.
               *
               * <p>Create a request for the method "assets.patch".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Patch#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Output only. The relative resource name of the asset, of the form:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{
               *     asset_id}
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset}
               * @since 1.13
               */
              protected Patch(
                  java.lang.String name,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Asset content) {
                super(
                    CloudDataplex.this,
                    "PATCH",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public Patch set$Xgafv(java.lang.String $Xgafv) {
                return (Patch) super.set$Xgafv($Xgafv);
              }

              @Override
              public Patch setAccessToken(java.lang.String accessToken) {
                return (Patch) super.setAccessToken(accessToken);
              }

              @Override
              public Patch setAlt(java.lang.String alt) {
                return (Patch) super.setAlt(alt);
              }

              @Override
              public Patch setCallback(java.lang.String callback) {
                return (Patch) super.setCallback(callback);
              }

              @Override
              public Patch setFields(java.lang.String fields) {
                return (Patch) super.setFields(fields);
              }

              @Override
              public Patch setKey(java.lang.String key) {
                return (Patch) super.setKey(key);
              }

              @Override
              public Patch setOauthToken(java.lang.String oauthToken) {
                return (Patch) super.setOauthToken(oauthToken);
              }

              @Override
              public Patch setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Patch) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Patch setQuotaUser(java.lang.String quotaUser) {
                return (Patch) super.setQuotaUser(quotaUser);
              }

              @Override
              public Patch setUploadType(java.lang.String uploadType) {
                return (Patch) super.setUploadType(uploadType);
              }

              @Override
              public Patch setUploadProtocol(java.lang.String uploadProtocol) {
                return (Patch) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Output only. The relative resource name of the asset, of the form: projects/{projec
               * t_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Output only. The relative resource name of the asset, of the form:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Output only. The relative resource name of the asset, of the form: projects/{projec
               * t_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
               */
              public Patch setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.name = name;
                return this;
              }

              /** Required. Mask of fields to update. */
              @com.google.api.client.util.Key private String updateMask;

              /** Required. Mask of fields to update. */
              public String getUpdateMask() {
                return updateMask;
              }

              /** Required. Mask of fields to update. */
              public Patch setUpdateMask(String updateMask) {
                this.updateMask = updateMask;
                return this;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public java.lang.Boolean getValidateOnly() {
                return validateOnly;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public Patch setValidateOnly(java.lang.Boolean validateOnly) {
                this.validateOnly = validateOnly;
                return this;
              }

              @Override
              public Patch set(String parameterName, Object value) {
                return (Patch) super.set(parameterName, value);
              }
            }
            /**
             * Marks actions associated with an asset as resolved(Deprecated).
             *
             * <p>Create a request for the method "assets.resolveAssetActions".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link ResolveAssetActions#execute()} method to invoke
             * the remote operation.
             *
             * @param asset Required. The name of the asset for which actions are being resolved of
             *     the form:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{
             *     asset_id}
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveAssetActionsRequest}
             * @return the request
             */
            public ResolveAssetActions resolveAssetActions(
                java.lang.String asset,
                com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ResolveAssetActionsRequest
                    content)
                throws java.io.IOException {
              ResolveAssetActions result = new ResolveAssetActions(asset, content);
              initialize(result);
              return result;
            }

            public class ResolveAssetActions
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

              private static final String REST_PATH = "v1/{+asset}:resolveAssetActions";

              private final java.util.regex.Pattern ASSET_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Marks actions associated with an asset as resolved(Deprecated).
               *
               * <p>Create a request for the method "assets.resolveAssetActions".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link ResolveAssetActions#execute()}
               * method to invoke the remote operation.
               *
               * <p>{@link ResolveAssetActions#initialize(com.google.api.client.googleapis.services.
               * AbstractGoogleClientRequest)} must be called to initialize this instance
               * immediately after invoking the constructor.
               *
               * @param asset Required. The name of the asset for which actions are being resolved
               *     of the form:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{
               *     asset_id}
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1ResolveAssetActionsRequest}
               * @since 1.13
               */
              protected ResolveAssetActions(
                  java.lang.String asset,
                  com.google.api.services.dataplex.v1.model
                          .GoogleCloudDataplexV1ResolveAssetActionsRequest
                      content) {
                super(
                    CloudDataplex.this,
                    "POST",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
                this.asset =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        asset, "Required parameter asset must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      ASSET_PATTERN.matcher(asset).matches(),
                      "Parameter asset must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public ResolveAssetActions set$Xgafv(java.lang.String $Xgafv) {
                return (ResolveAssetActions) super.set$Xgafv($Xgafv);
              }

              @Override
              public ResolveAssetActions setAccessToken(java.lang.String accessToken) {
                return (ResolveAssetActions) super.setAccessToken(accessToken);
              }

              @Override
              public ResolveAssetActions setAlt(java.lang.String alt) {
                return (ResolveAssetActions) super.setAlt(alt);
              }

              @Override
              public ResolveAssetActions setCallback(java.lang.String callback) {
                return (ResolveAssetActions) super.setCallback(callback);
              }

              @Override
              public ResolveAssetActions setFields(java.lang.String fields) {
                return (ResolveAssetActions) super.setFields(fields);
              }

              @Override
              public ResolveAssetActions setKey(java.lang.String key) {
                return (ResolveAssetActions) super.setKey(key);
              }

              @Override
              public ResolveAssetActions setOauthToken(java.lang.String oauthToken) {
                return (ResolveAssetActions) super.setOauthToken(oauthToken);
              }

              @Override
              public ResolveAssetActions setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (ResolveAssetActions) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public ResolveAssetActions setQuotaUser(java.lang.String quotaUser) {
                return (ResolveAssetActions) super.setQuotaUser(quotaUser);
              }

              @Override
              public ResolveAssetActions setUploadType(java.lang.String uploadType) {
                return (ResolveAssetActions) super.setUploadType(uploadType);
              }

              @Override
              public ResolveAssetActions setUploadProtocol(java.lang.String uploadProtocol) {
                return (ResolveAssetActions) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The name of the asset for which actions are being resolved of the form: p
               * rojects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/as
               * sets/{asset_id}
               */
              @com.google.api.client.util.Key private java.lang.String asset;

              /**
               * Required. The name of the asset for which actions are being resolved of the form:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
               */
              public java.lang.String getAsset() {
                return asset;
              }

              /**
               * Required. The name of the asset for which actions are being resolved of the form: p
               * rojects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/as
               * sets/{asset_id}
               */
              public ResolveAssetActions setAsset(java.lang.String asset) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      ASSET_PATTERN.matcher(asset).matches(),
                      "Parameter asset must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.asset = asset;
                return this;
              }

              @Override
              public ResolveAssetActions set(String parameterName, Object value) {
                return (ResolveAssetActions) super.set(parameterName, value);
              }
            }
            /**
             * Sets the access control policy on the specified resource. Replaces any existing
             * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
             *
             * <p>Create a request for the method "assets.setIamPolicy".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link SetIamPolicy#execute()} method to invoke the
             * remote operation.
             *
             * @param resource REQUIRED: The resource for which the policy is being specified. See
             *     the operation documentation for the appropriate value for this field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
             * @return the request
             */
            public SetIamPolicy setIamPolicy(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest content)
                throws java.io.IOException {
              SetIamPolicy result = new SetIamPolicy(resource, content);
              initialize(result);
              return result;
            }

            public class SetIamPolicy
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleIamV1Policy> {

              private static final String REST_PATH = "v1/{+resource}:setIamPolicy";

              private final java.util.regex.Pattern RESOURCE_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Sets the access control policy on the specified resource. Replaces any existing
               * policy.Can return NOT_FOUND, INVALID_ARGUMENT, and PERMISSION_DENIED errors.
               *
               * <p>Create a request for the method "assets.setIamPolicy".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link SetIamPolicy#execute()} method to
               * invoke the remote operation.
               *
               * <p>{@link
               * SetIamPolicy#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param resource REQUIRED: The resource for which the policy is being specified. See
               *     the operation documentation for the appropriate value for this field.
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest}
               * @since 1.13
               */
              protected SetIamPolicy(
                  java.lang.String resource,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1SetIamPolicyRequest
                      content) {
                super(
                    CloudDataplex.this,
                    "POST",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleIamV1Policy.class);
                this.resource =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        resource, "Required parameter resource must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      RESOURCE_PATTERN.matcher(resource).matches(),
                      "Parameter resource must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public SetIamPolicy set$Xgafv(java.lang.String $Xgafv) {
                return (SetIamPolicy) super.set$Xgafv($Xgafv);
              }

              @Override
              public SetIamPolicy setAccessToken(java.lang.String accessToken) {
                return (SetIamPolicy) super.setAccessToken(accessToken);
              }

              @Override
              public SetIamPolicy setAlt(java.lang.String alt) {
                return (SetIamPolicy) super.setAlt(alt);
              }

              @Override
              public SetIamPolicy setCallback(java.lang.String callback) {
                return (SetIamPolicy) super.setCallback(callback);
              }

              @Override
              public SetIamPolicy setFields(java.lang.String fields) {
                return (SetIamPolicy) super.setFields(fields);
              }

              @Override
              public SetIamPolicy setKey(java.lang.String key) {
                return (SetIamPolicy) super.setKey(key);
              }

              @Override
              public SetIamPolicy setOauthToken(java.lang.String oauthToken) {
                return (SetIamPolicy) super.setOauthToken(oauthToken);
              }

              @Override
              public SetIamPolicy setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (SetIamPolicy) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public SetIamPolicy setQuotaUser(java.lang.String quotaUser) {
                return (SetIamPolicy) super.setQuotaUser(quotaUser);
              }

              @Override
              public SetIamPolicy setUploadType(java.lang.String uploadType) {
                return (SetIamPolicy) super.setUploadType(uploadType);
              }

              @Override
              public SetIamPolicy setUploadProtocol(java.lang.String uploadProtocol) {
                return (SetIamPolicy) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * REQUIRED: The resource for which the policy is being specified. See the operation
               * documentation for the appropriate value for this field.
               */
              @com.google.api.client.util.Key private java.lang.String resource;

              /**
               * REQUIRED: The resource for which the policy is being specified. See the operation
               * documentation for the appropriate value for this field.
               */
              public java.lang.String getResource() {
                return resource;
              }

              /**
               * REQUIRED: The resource for which the policy is being specified. See the operation
               * documentation for the appropriate value for this field.
               */
              public SetIamPolicy setResource(java.lang.String resource) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      RESOURCE_PATTERN.matcher(resource).matches(),
                      "Parameter resource must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.resource = resource;
                return this;
              }

              @Override
              public SetIamPolicy set(String parameterName, Object value) {
                return (SetIamPolicy) super.set(parameterName, value);
              }
            }
            /**
             * Returns permissions that a caller has on the specified resource. If the resource does
             * not exist, this will return an empty set of permissions, not a NOT_FOUND error.Note:
             * This operation is designed to be used for building permission-aware UIs and
             * command-line tools, not for authorization checking. This operation may "fail open"
             * without warning.
             *
             * <p>Create a request for the method "assets.testIamPermissions".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link TestIamPermissions#execute()} method to invoke
             * the remote operation.
             *
             * @param resource REQUIRED: The resource for which the policy detail is being
             *     requested. See the operation documentation for the appropriate value for this
             *     field.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
             * @return the request
             */
            public TestIamPermissions testIamPermissions(
                java.lang.String resource,
                com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                    content)
                throws java.io.IOException {
              TestIamPermissions result = new TestIamPermissions(resource, content);
              initialize(result);
              return result;
            }

            public class TestIamPermissions
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model
                        .GoogleIamV1TestIamPermissionsResponse> {

              private static final String REST_PATH = "v1/{+resource}:testIamPermissions";

              private final java.util.regex.Pattern RESOURCE_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

              /**
               * Returns permissions that a caller has on the specified resource. If the resource
               * does not exist, this will return an empty set of permissions, not a NOT_FOUND
               * error.Note: This operation is designed to be used for building permission-aware UIs
               * and command-line tools, not for authorization checking. This operation may "fail
               * open" without warning.
               *
               * <p>Create a request for the method "assets.testIamPermissions".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link TestIamPermissions#execute()}
               * method to invoke the remote operation.
               *
               * <p>{@link TestIamPermissions#initialize(com.google.api.client.googleapis.services.A
               * bstractGoogleClientRequest)} must be called to initialize this instance immediately
               * after invoking the constructor.
               *
               * @param resource REQUIRED: The resource for which the policy detail is being
               *     requested. See the operation documentation for the appropriate value for this
               *     field.
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest}
               * @since 1.13
               */
              protected TestIamPermissions(
                  java.lang.String resource,
                  com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsRequest
                      content) {
                super(
                    CloudDataplex.this,
                    "POST",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleIamV1TestIamPermissionsResponse
                        .class);
                this.resource =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        resource, "Required parameter resource must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      RESOURCE_PATTERN.matcher(resource).matches(),
                      "Parameter resource must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
              }

              @Override
              public TestIamPermissions set$Xgafv(java.lang.String $Xgafv) {
                return (TestIamPermissions) super.set$Xgafv($Xgafv);
              }

              @Override
              public TestIamPermissions setAccessToken(java.lang.String accessToken) {
                return (TestIamPermissions) super.setAccessToken(accessToken);
              }

              @Override
              public TestIamPermissions setAlt(java.lang.String alt) {
                return (TestIamPermissions) super.setAlt(alt);
              }

              @Override
              public TestIamPermissions setCallback(java.lang.String callback) {
                return (TestIamPermissions) super.setCallback(callback);
              }

              @Override
              public TestIamPermissions setFields(java.lang.String fields) {
                return (TestIamPermissions) super.setFields(fields);
              }

              @Override
              public TestIamPermissions setKey(java.lang.String key) {
                return (TestIamPermissions) super.setKey(key);
              }

              @Override
              public TestIamPermissions setOauthToken(java.lang.String oauthToken) {
                return (TestIamPermissions) super.setOauthToken(oauthToken);
              }

              @Override
              public TestIamPermissions setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (TestIamPermissions) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public TestIamPermissions setQuotaUser(java.lang.String quotaUser) {
                return (TestIamPermissions) super.setQuotaUser(quotaUser);
              }

              @Override
              public TestIamPermissions setUploadType(java.lang.String uploadType) {
                return (TestIamPermissions) super.setUploadType(uploadType);
              }

              @Override
              public TestIamPermissions setUploadProtocol(java.lang.String uploadProtocol) {
                return (TestIamPermissions) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * REQUIRED: The resource for which the policy detail is being requested. See the
               * operation documentation for the appropriate value for this field.
               */
              @com.google.api.client.util.Key private java.lang.String resource;

              /**
               * REQUIRED: The resource for which the policy detail is being requested. See the
               * operation documentation for the appropriate value for this field.
               */
              public java.lang.String getResource() {
                return resource;
              }

              /**
               * REQUIRED: The resource for which the policy detail is being requested. See the
               * operation documentation for the appropriate value for this field.
               */
              public TestIamPermissions setResource(java.lang.String resource) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      RESOURCE_PATTERN.matcher(resource).matches(),
                      "Parameter resource must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                }
                this.resource = resource;
                return this;
              }

              @Override
              public TestIamPermissions set(String parameterName, Object value) {
                return (TestIamPermissions) super.set(parameterName, value);
              }
            }

            /**
             * An accessor for creating requests from the Actions collection.
             *
             * <p>The typical use is:
             *
             * <pre>
             *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
             *   {@code CloudDataplex.Actions.List request = dataplex.actions().list(parameters ...)}
             * </pre>
             *
             * @return the resource collection
             */
            public Actions actions() {
              return new Actions();
            }

            /** The "actions" collection of methods. */
            public class Actions {

              /**
               * Lists action resources in an asset.
               *
               * <p>Create a request for the method "actions.list".
               *
               * <p>This request holds the parameters needed by the dataplex server. After setting
               * any optional parameters, call the {@link List#execute()} method to invoke the
               * remote operation.
               *
               * @param parent Required. The resource name of the parent asset:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{
               *     asset_id}
               * @return the request
               */
              public List list(java.lang.String parent) throws java.io.IOException {
                List result = new List(parent);
                initialize(result);
                return result;
              }

              public class List
                  extends CloudDataplexRequest<
                      com.google.api.services.dataplex.v1.model
                          .GoogleCloudDataplexV1ListActionsResponse> {

                private static final String REST_PATH = "v1/{+parent}/actions";

                private final java.util.regex.Pattern PARENT_PATTERN =
                    java.util.regex.Pattern.compile(
                        "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");

                /**
                 * Lists action resources in an asset.
                 *
                 * <p>Create a request for the method "actions.list".
                 *
                 * <p>This request holds the parameters needed by the the dataplex server. After
                 * setting any optional parameters, call the {@link List#execute()} method to invoke
                 * the remote operation.
                 *
                 * <p>{@link
                 * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
                 * must be called to initialize this instance immediately after invoking the
                 * constructor.
                 *
                 * @param parent Required. The resource name of the parent asset:
                 *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{
                 *     asset_id}
                 * @since 1.13
                 */
                protected List(java.lang.String parent) {
                  super(
                      CloudDataplex.this,
                      "GET",
                      REST_PATH,
                      null,
                      com.google.api.services.dataplex.v1.model
                          .GoogleCloudDataplexV1ListActionsResponse.class);
                  this.parent =
                      com.google.api.client.util.Preconditions.checkNotNull(
                          parent, "Required parameter parent must be specified.");
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        PARENT_PATTERN.matcher(parent).matches(),
                        "Parameter parent must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                  }
                }

                @Override
                public com.google.api.client.http.HttpResponse executeUsingHead()
                    throws java.io.IOException {
                  return super.executeUsingHead();
                }

                @Override
                public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                    throws java.io.IOException {
                  return super.buildHttpRequestUsingHead();
                }

                @Override
                public List set$Xgafv(java.lang.String $Xgafv) {
                  return (List) super.set$Xgafv($Xgafv);
                }

                @Override
                public List setAccessToken(java.lang.String accessToken) {
                  return (List) super.setAccessToken(accessToken);
                }

                @Override
                public List setAlt(java.lang.String alt) {
                  return (List) super.setAlt(alt);
                }

                @Override
                public List setCallback(java.lang.String callback) {
                  return (List) super.setCallback(callback);
                }

                @Override
                public List setFields(java.lang.String fields) {
                  return (List) super.setFields(fields);
                }

                @Override
                public List setKey(java.lang.String key) {
                  return (List) super.setKey(key);
                }

                @Override
                public List setOauthToken(java.lang.String oauthToken) {
                  return (List) super.setOauthToken(oauthToken);
                }

                @Override
                public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                  return (List) super.setPrettyPrint(prettyPrint);
                }

                @Override
                public List setQuotaUser(java.lang.String quotaUser) {
                  return (List) super.setQuotaUser(quotaUser);
                }

                @Override
                public List setUploadType(java.lang.String uploadType) {
                  return (List) super.setUploadType(uploadType);
                }

                @Override
                public List setUploadProtocol(java.lang.String uploadProtocol) {
                  return (List) super.setUploadProtocol(uploadProtocol);
                }

                /**
                 * Required. The resource name of the parent asset: projects/{project_number}/locati
                 * ons/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
                 */
                @com.google.api.client.util.Key private java.lang.String parent;

                /**
                 * Required. The resource name of the parent asset:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
                 */
                public java.lang.String getParent() {
                  return parent;
                }

                /**
                 * Required. The resource name of the parent asset: projects/{project_number}/locati
                 * ons/{location_id}/lakes/{lake_id}/zones/{zone_id}/assets/{asset_id}
                 */
                public List setParent(java.lang.String parent) {
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        PARENT_PATTERN.matcher(parent).matches(),
                        "Parameter parent must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/assets/[^/]+$");
                  }
                  this.parent = parent;
                  return this;
                }

                /**
                 * Optional. Maximum number of actions to return. The service may return fewer than
                 * this value. If unspecified, at most 10 actions will be returned. The maximum
                 * value is 1000; values above 1000 will be coerced to 1000.
                 */
                @com.google.api.client.util.Key private java.lang.Integer pageSize;

                /**
                 * Optional. Maximum number of actions to return. The service may return fewer than
                 * this value. If unspecified, at most 10 actions will be returned. The maximum
                 * value is 1000; values above 1000 will be coerced to 1000.
                 */
                public java.lang.Integer getPageSize() {
                  return pageSize;
                }

                /**
                 * Optional. Maximum number of actions to return. The service may return fewer than
                 * this value. If unspecified, at most 10 actions will be returned. The maximum
                 * value is 1000; values above 1000 will be coerced to 1000.
                 */
                public List setPageSize(java.lang.Integer pageSize) {
                  this.pageSize = pageSize;
                  return this;
                }

                /**
                 * Optional. Page token received from a previous ListAssetActions call. Provide this
                 * to retrieve the subsequent page. When paginating, all other parameters provided
                 * to ListAssetActions must match the call that provided the page token.
                 */
                @com.google.api.client.util.Key private java.lang.String pageToken;

                /**
                 * Optional. Page token received from a previous ListAssetActions call. Provide this
                 * to retrieve the subsequent page. When paginating, all other parameters provided
                 * to ListAssetActions must match the call that provided the page token.
                 */
                public java.lang.String getPageToken() {
                  return pageToken;
                }

                /**
                 * Optional. Page token received from a previous ListAssetActions call. Provide this
                 * to retrieve the subsequent page. When paginating, all other parameters provided
                 * to ListAssetActions must match the call that provided the page token.
                 */
                public List setPageToken(java.lang.String pageToken) {
                  this.pageToken = pageToken;
                  return this;
                }

                @Override
                public List set(String parameterName, Object value) {
                  return (List) super.set(parameterName, value);
                }
              }
            }
          }
          /**
           * An accessor for creating requests from the Entities collection.
           *
           * <p>The typical use is:
           *
           * <pre>
           *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
           *   {@code CloudDataplex.Entities.List request = dataplex.entities().list(parameters ...)}
           * </pre>
           *
           * @return the resource collection
           */
          public Entities entities() {
            return new Entities();
          }

          /** The "entities" collection of methods. */
          public class Entities {

            /**
             * Create a metadata entity.
             *
             * <p>Create a request for the method "entities.create".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Create#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}`
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity}
             * @return the request
             */
            public Create create(
                java.lang.String parent,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity content)
                throws java.io.IOException {
              Create result = new Create(parent, content);
              initialize(result);
              return result;
            }

            public class Create
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity> {

              private static final String REST_PATH = "v1/{+parent}/entities";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

              /**
               * Create a metadata entity.
               *
               * <p>Create a request for the method "entities.create".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Create#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent zone:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}`
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity}
               * @since 1.13
               */
              protected Create(
                  java.lang.String parent,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity content) {
                super(
                    CloudDataplex.this,
                    "POST",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity.class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
              }

              @Override
              public Create set$Xgafv(java.lang.String $Xgafv) {
                return (Create) super.set$Xgafv($Xgafv);
              }

              @Override
              public Create setAccessToken(java.lang.String accessToken) {
                return (Create) super.setAccessToken(accessToken);
              }

              @Override
              public Create setAlt(java.lang.String alt) {
                return (Create) super.setAlt(alt);
              }

              @Override
              public Create setCallback(java.lang.String callback) {
                return (Create) super.setCallback(callback);
              }

              @Override
              public Create setFields(java.lang.String fields) {
                return (Create) super.setFields(fields);
              }

              @Override
              public Create setKey(java.lang.String key) {
                return (Create) super.setKey(key);
              }

              @Override
              public Create setOauthToken(java.lang.String oauthToken) {
                return (Create) super.setOauthToken(oauthToken);
              }

              @Override
              public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Create) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Create setQuotaUser(java.lang.String quotaUser) {
                return (Create) super.setQuotaUser(quotaUser);
              }

              @Override
              public Create setUploadType(java.lang.String uploadType) {
                return (Create) super.setUploadType(uploadType);
              }

              @Override
              public Create setUploadProtocol(java.lang.String uploadProtocol) {
                return (Create) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public Create setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /** Optional. Entity identifier. */
              @com.google.api.client.util.Key private java.lang.String entityId;

              /** Optional. Entity identifier. */
              public java.lang.String getEntityId() {
                return entityId;
              }

              /** Optional. Entity identifier. */
              public Create setEntityId(java.lang.String entityId) {
                this.entityId = entityId;
                return this;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public java.lang.Boolean getValidateOnly() {
                return validateOnly;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public Create setValidateOnly(java.lang.Boolean validateOnly) {
                this.validateOnly = validateOnly;
                return this;
              }

              @Override
              public Create set(String parameterName, Object value) {
                return (Create) super.set(parameterName, value);
              }
            }
            /**
             * Delete a metadata entity.
             *
             * <p>Create a request for the method "entities.delete".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Delete#execute()} method to invoke the remote
             * operation.
             *
             * @param name Required. The resource name of the entity:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}/entities/{entity_id}
             * @return the request
             */
            public Delete delete(java.lang.String name) throws java.io.IOException {
              Delete result = new Delete(name);
              initialize(result);
              return result;
            }

            public class Delete
                extends CloudDataplexRequest<com.google.api.services.dataplex.v1.model.Empty> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");

              /**
               * Delete a metadata entity.
               *
               * <p>Create a request for the method "entities.delete".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Delete#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Required. The resource name of the entity:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/entities/{entity_id}
               * @since 1.13
               */
              protected Delete(java.lang.String name) {
                super(
                    CloudDataplex.this,
                    "DELETE",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.Empty.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                }
              }

              @Override
              public Delete set$Xgafv(java.lang.String $Xgafv) {
                return (Delete) super.set$Xgafv($Xgafv);
              }

              @Override
              public Delete setAccessToken(java.lang.String accessToken) {
                return (Delete) super.setAccessToken(accessToken);
              }

              @Override
              public Delete setAlt(java.lang.String alt) {
                return (Delete) super.setAlt(alt);
              }

              @Override
              public Delete setCallback(java.lang.String callback) {
                return (Delete) super.setCallback(callback);
              }

              @Override
              public Delete setFields(java.lang.String fields) {
                return (Delete) super.setFields(fields);
              }

              @Override
              public Delete setKey(java.lang.String key) {
                return (Delete) super.setKey(key);
              }

              @Override
              public Delete setOauthToken(java.lang.String oauthToken) {
                return (Delete) super.setOauthToken(oauthToken);
              }

              @Override
              public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Delete) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Delete setQuotaUser(java.lang.String quotaUser) {
                return (Delete) super.setQuotaUser(quotaUser);
              }

              @Override
              public Delete setUploadType(java.lang.String uploadType) {
                return (Delete) super.setUploadType(uploadType);
              }

              @Override
              public Delete setUploadProtocol(java.lang.String uploadProtocol) {
                return (Delete) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the entity:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/entities/{entity_id}
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Required. The resource name of the entity:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/entities/{entity_id}
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Required. The resource name of the entity:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/entities/{entity_id}
               */
              public Delete setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                }
                this.name = name;
                return this;
              }

              /**
               * Optional. The etag associated with the partition if it was previously retrieved.
               */
              @com.google.api.client.util.Key private java.lang.String etag;

              /**
               * Optional. The etag associated with the partition if it was previously retrieved.
               */
              public java.lang.String getEtag() {
                return etag;
              }

              /**
               * Optional. The etag associated with the partition if it was previously retrieved.
               */
              public Delete setEtag(java.lang.String etag) {
                this.etag = etag;
                return this;
              }

              @Override
              public Delete set(String parameterName, Object value) {
                return (Delete) super.set(parameterName, value);
              }
            }
            /**
             * Get a metadata entity.
             *
             * <p>Create a request for the method "entities.get".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * @param name Required. The resource name of the entity:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}/entities/{entity_id_or_entity_uid} The entity id part could
             *     either be the entity unique ID or the user provided ID.
             * @return the request
             */
            public Get get(java.lang.String name) throws java.io.IOException {
              Get result = new Get(name);
              initialize(result);
              return result;
            }

            public class Get
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");

              /**
               * Get a metadata entity.
               *
               * <p>Create a request for the method "entities.get".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Get#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Required. The resource name of the entity:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/entities/{entity_id_or_entity_uid} The entity id part could
               *     either be the entity unique ID or the user provided ID.
               * @since 1.13
               */
              protected Get(java.lang.String name) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public Get set$Xgafv(java.lang.String $Xgafv) {
                return (Get) super.set$Xgafv($Xgafv);
              }

              @Override
              public Get setAccessToken(java.lang.String accessToken) {
                return (Get) super.setAccessToken(accessToken);
              }

              @Override
              public Get setAlt(java.lang.String alt) {
                return (Get) super.setAlt(alt);
              }

              @Override
              public Get setCallback(java.lang.String callback) {
                return (Get) super.setCallback(callback);
              }

              @Override
              public Get setFields(java.lang.String fields) {
                return (Get) super.setFields(fields);
              }

              @Override
              public Get setKey(java.lang.String key) {
                return (Get) super.setKey(key);
              }

              @Override
              public Get setOauthToken(java.lang.String oauthToken) {
                return (Get) super.setOauthToken(oauthToken);
              }

              @Override
              public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Get) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Get setQuotaUser(java.lang.String quotaUser) {
                return (Get) super.setQuotaUser(quotaUser);
              }

              @Override
              public Get setUploadType(java.lang.String uploadType) {
                return (Get) super.setUploadType(uploadType);
              }

              @Override
              public Get setUploadProtocol(java.lang.String uploadProtocol) {
                return (Get) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the entity:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/entities/{entity_id_or_entity_uid} The entity id part could either
               * be the entity unique ID or the user provided ID.
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Required. The resource name of the entity:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/entities/{entity_id_or_entity_uid} The entity id part could either
               * be the entity unique ID or the user provided ID.
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Required. The resource name of the entity:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               * /zones/{zone_id}/entities/{entity_id_or_entity_uid} The entity id part could either
               * be the entity unique ID or the user provided ID.
               */
              public Get setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                }
                this.name = name;
                return this;
              }

              /**
               * Optional. Used to select the subset of information about the Entity to return.
               * Defaults to BASIC.
               */
              @com.google.api.client.util.Key private java.lang.String view;

              /**
               * Optional. Used to select the subset of information about the Entity to return.
               * Defaults to BASIC.
               */
              public java.lang.String getView() {
                return view;
              }

              /**
               * Optional. Used to select the subset of information about the Entity to return.
               * Defaults to BASIC.
               */
              public Get setView(java.lang.String view) {
                this.view = view;
                return this;
              }

              @Override
              public Get set(String parameterName, Object value) {
                return (Get) super.set(parameterName, value);
              }
            }
            /**
             * List metadata entities of a given zone.
             *
             * <p>Create a request for the method "entities.list".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * @param parent Required. The resource name of the parent zone:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
             *     /zones/{zone_id}`
             * @return the request
             */
            public List list(java.lang.String parent) throws java.io.IOException {
              List result = new List(parent);
              initialize(result);
              return result;
            }

            public class List
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListEntitiesResponse> {

              private static final String REST_PATH = "v1/{+parent}/entities";

              private final java.util.regex.Pattern PARENT_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");

              /**
               * List metadata entities of a given zone.
               *
               * <p>Create a request for the method "entities.list".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link List#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param parent Required. The resource name of the parent zone:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}`
               * @since 1.13
               */
              protected List(java.lang.String parent) {
                super(
                    CloudDataplex.this,
                    "GET",
                    REST_PATH,
                    null,
                    com.google.api.services.dataplex.v1.model
                        .GoogleCloudDataplexV1ListEntitiesResponse.class);
                this.parent =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        parent, "Required parameter parent must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
              }

              @Override
              public com.google.api.client.http.HttpResponse executeUsingHead()
                  throws java.io.IOException {
                return super.executeUsingHead();
              }

              @Override
              public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                  throws java.io.IOException {
                return super.buildHttpRequestUsingHead();
              }

              @Override
              public List set$Xgafv(java.lang.String $Xgafv) {
                return (List) super.set$Xgafv($Xgafv);
              }

              @Override
              public List setAccessToken(java.lang.String accessToken) {
                return (List) super.setAccessToken(accessToken);
              }

              @Override
              public List setAlt(java.lang.String alt) {
                return (List) super.setAlt(alt);
              }

              @Override
              public List setCallback(java.lang.String callback) {
                return (List) super.setCallback(callback);
              }

              @Override
              public List setFields(java.lang.String fields) {
                return (List) super.setFields(fields);
              }

              @Override
              public List setKey(java.lang.String key) {
                return (List) super.setKey(key);
              }

              @Override
              public List setOauthToken(java.lang.String oauthToken) {
                return (List) super.setOauthToken(oauthToken);
              }

              @Override
              public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (List) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public List setQuotaUser(java.lang.String quotaUser) {
                return (List) super.setQuotaUser(quotaUser);
              }

              @Override
              public List setUploadType(java.lang.String uploadType) {
                return (List) super.setUploadType(uploadType);
              }

              @Override
              public List setUploadProtocol(java.lang.String uploadProtocol) {
                return (List) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              @com.google.api.client.util.Key private java.lang.String parent;

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public java.lang.String getParent() {
                return parent;
              }

              /**
               * Required. The resource name of the parent zone:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id} /zones/{zone_id}`
               */
              public List setParent(java.lang.String parent) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      PARENT_PATTERN.matcher(parent).matches(),
                      "Parameter parent must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+$");
                }
                this.parent = parent;
                return this;
              }

              /** Optional. Filter request by name prefix. */
              @com.google.api.client.util.Key private java.lang.String filter;

              /** Optional. Filter request by name prefix. */
              public java.lang.String getFilter() {
                return filter;
              }

              /** Optional. Filter request by name prefix. */
              public List setFilter(java.lang.String filter) {
                this.filter = filter;
                return this;
              }

              /**
               * Optional. Maximum number of entities to return. The service may return fewer than
               * this value. If unspecified, at most 10 entities will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              @com.google.api.client.util.Key private java.lang.Integer pageSize;

              /**
               * Optional. Maximum number of entities to return. The service may return fewer than
               * this value. If unspecified, at most 10 entities will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              public java.lang.Integer getPageSize() {
                return pageSize;
              }

              /**
               * Optional. Maximum number of entities to return. The service may return fewer than
               * this value. If unspecified, at most 10 entities will be returned. The maximum value
               * is 1000; values above 1000 will be coerced to 1000.
               */
              public List setPageSize(java.lang.Integer pageSize) {
                this.pageSize = pageSize;
                return this;
              }

              /**
               * Optional. Page token received from a previous ListEntities call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListEntities must match the call that provided the page token.
               */
              @com.google.api.client.util.Key private java.lang.String pageToken;

              /**
               * Optional. Page token received from a previous ListEntities call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListEntities must match the call that provided the page token.
               */
              public java.lang.String getPageToken() {
                return pageToken;
              }

              /**
               * Optional. Page token received from a previous ListEntities call. Provide this to
               * retrieve the subsequent page. When paginating, all other parameters provided to
               * ListEntities must match the call that provided the page token.
               */
              public List setPageToken(java.lang.String pageToken) {
                this.pageToken = pageToken;
                return this;
              }

              /** Required. Specify the entity view to make a partial list request. */
              @com.google.api.client.util.Key private java.lang.String view;

              /** Required. Specify the entity view to make a partial list request. */
              public java.lang.String getView() {
                return view;
              }

              /** Required. Specify the entity view to make a partial list request. */
              public List setView(java.lang.String view) {
                this.view = view;
                return this;
              }

              @Override
              public List set(String parameterName, Object value) {
                return (List) super.set(parameterName, value);
              }
            }
            /**
             * Update a metadata entity. Only supports full resource update.
             *
             * <p>Create a request for the method "entities.update".
             *
             * <p>This request holds the parameters needed by the dataplex server. After setting any
             * optional parameters, call the {@link Update#execute()} method to invoke the remote
             * operation.
             *
             * @param name Output only. Immutable. The resource name of the entity, of the form:
             *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities
             *     /{entity} The {entity} is a generated unique ID.
             * @param content the {@link
             *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity}
             * @return the request
             */
            public Update update(
                java.lang.String name,
                com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity content)
                throws java.io.IOException {
              Update result = new Update(name, content);
              initialize(result);
              return result;
            }

            public class Update
                extends CloudDataplexRequest<
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity> {

              private static final String REST_PATH = "v1/{+name}";

              private final java.util.regex.Pattern NAME_PATTERN =
                  java.util.regex.Pattern.compile(
                      "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");

              /**
               * Update a metadata entity. Only supports full resource update.
               *
               * <p>Create a request for the method "entities.update".
               *
               * <p>This request holds the parameters needed by the the dataplex server. After
               * setting any optional parameters, call the {@link Update#execute()} method to invoke
               * the remote operation.
               *
               * <p>{@link
               * Update#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
               * must be called to initialize this instance immediately after invoking the
               * constructor.
               *
               * @param name Output only. Immutable. The resource name of the entity, of the form:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities
               *     /{entity} The {entity} is a generated unique ID.
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity}
               * @since 1.13
               */
              protected Update(
                  java.lang.String name,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity content) {
                super(
                    CloudDataplex.this,
                    "PUT",
                    REST_PATH,
                    content,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Entity.class);
                this.name =
                    com.google.api.client.util.Preconditions.checkNotNull(
                        name, "Required parameter name must be specified.");
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                }
              }

              @Override
              public Update set$Xgafv(java.lang.String $Xgafv) {
                return (Update) super.set$Xgafv($Xgafv);
              }

              @Override
              public Update setAccessToken(java.lang.String accessToken) {
                return (Update) super.setAccessToken(accessToken);
              }

              @Override
              public Update setAlt(java.lang.String alt) {
                return (Update) super.setAlt(alt);
              }

              @Override
              public Update setCallback(java.lang.String callback) {
                return (Update) super.setCallback(callback);
              }

              @Override
              public Update setFields(java.lang.String fields) {
                return (Update) super.setFields(fields);
              }

              @Override
              public Update setKey(java.lang.String key) {
                return (Update) super.setKey(key);
              }

              @Override
              public Update setOauthToken(java.lang.String oauthToken) {
                return (Update) super.setOauthToken(oauthToken);
              }

              @Override
              public Update setPrettyPrint(java.lang.Boolean prettyPrint) {
                return (Update) super.setPrettyPrint(prettyPrint);
              }

              @Override
              public Update setQuotaUser(java.lang.String quotaUser) {
                return (Update) super.setQuotaUser(quotaUser);
              }

              @Override
              public Update setUploadType(java.lang.String uploadType) {
                return (Update) super.setUploadType(uploadType);
              }

              @Override
              public Update setUploadProtocol(java.lang.String uploadProtocol) {
                return (Update) super.setUploadProtocol(uploadProtocol);
              }

              /**
               * Output only. Immutable. The resource name of the entity, of the form: projects/{pro
               * ject_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{enti
               * ty} The {entity} is a generated unique ID.
               */
              @com.google.api.client.util.Key private java.lang.String name;

              /**
               * Output only. Immutable. The resource name of the entity, of the form:
               * projects/{project_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}
               * The {entity} is a generated unique ID.
               */
              public java.lang.String getName() {
                return name;
              }

              /**
               * Output only. Immutable. The resource name of the entity, of the form: projects/{pro
               * ject_number}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{enti
               * ty} The {entity} is a generated unique ID.
               */
              public Update setName(java.lang.String name) {
                if (!getSuppressPatternChecks()) {
                  com.google.api.client.util.Preconditions.checkArgument(
                      NAME_PATTERN.matcher(name).matches(),
                      "Parameter name must conform to the pattern "
                          + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                }
                this.name = name;
                return this;
              }

              /** Required. Mask of fields to update. */
              @com.google.api.client.util.Key private String updateMask;

              /** Required. Mask of fields to update. */
              public String getUpdateMask() {
                return updateMask;
              }

              /** Required. Mask of fields to update. */
              public Update setUpdateMask(String updateMask) {
                this.updateMask = updateMask;
                return this;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public java.lang.Boolean getValidateOnly() {
                return validateOnly;
              }

              /**
               * Optional. Only validate the request, but do not perform mutations. The default is
               * false.
               */
              public Update setValidateOnly(java.lang.Boolean validateOnly) {
                this.validateOnly = validateOnly;
                return this;
              }

              @Override
              public Update set(String parameterName, Object value) {
                return (Update) super.set(parameterName, value);
              }
            }

            /**
             * An accessor for creating requests from the Partitions collection.
             *
             * <p>The typical use is:
             *
             * <pre>
             *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
             *   {@code CloudDataplex.Partitions.List request = dataplex.partitions().list(parameters ...)}
             * </pre>
             *
             * @return the resource collection
             */
            public Partitions partitions() {
              return new Partitions();
            }

            /** The "partitions" collection of methods. */
            public class Partitions {

              /**
               * Create an metadata partition.
               *
               * <p>Create a request for the method "partitions.create".
               *
               * <p>This request holds the parameters needed by the dataplex server. After setting
               * any optional parameters, call the {@link Create#execute()} method to invoke the
               * remote operation.
               *
               * @param parent Required. The resource name of the parent zone:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/entities/{entity_id}`
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition}
               * @return the request
               */
              public Create create(
                  java.lang.String parent,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition content)
                  throws java.io.IOException {
                Create result = new Create(parent, content);
                initialize(result);
                return result;
              }

              public class Create
                  extends CloudDataplexRequest<
                      com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition> {

                private static final String REST_PATH = "v1/{+parent}/partitions";

                private final java.util.regex.Pattern PARENT_PATTERN =
                    java.util.regex.Pattern.compile(
                        "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");

                /**
                 * Create an metadata partition.
                 *
                 * <p>Create a request for the method "partitions.create".
                 *
                 * <p>This request holds the parameters needed by the the dataplex server. After
                 * setting any optional parameters, call the {@link Create#execute()} method to
                 * invoke the remote operation.
                 *
                 * <p>{@link
                 * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
                 * must be called to initialize this instance immediately after invoking the
                 * constructor.
                 *
                 * @param parent Required. The resource name of the parent zone:
                 *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 *     /zones/{zone_id}/entities/{entity_id}`
                 * @param content the {@link
                 *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition}
                 * @since 1.13
                 */
                protected Create(
                    java.lang.String parent,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition
                        content) {
                  super(
                      CloudDataplex.this,
                      "POST",
                      REST_PATH,
                      content,
                      com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition
                          .class);
                  this.parent =
                      com.google.api.client.util.Preconditions.checkNotNull(
                          parent, "Required parameter parent must be specified.");
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        PARENT_PATTERN.matcher(parent).matches(),
                        "Parameter parent must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                  }
                }

                @Override
                public Create set$Xgafv(java.lang.String $Xgafv) {
                  return (Create) super.set$Xgafv($Xgafv);
                }

                @Override
                public Create setAccessToken(java.lang.String accessToken) {
                  return (Create) super.setAccessToken(accessToken);
                }

                @Override
                public Create setAlt(java.lang.String alt) {
                  return (Create) super.setAlt(alt);
                }

                @Override
                public Create setCallback(java.lang.String callback) {
                  return (Create) super.setCallback(callback);
                }

                @Override
                public Create setFields(java.lang.String fields) {
                  return (Create) super.setFields(fields);
                }

                @Override
                public Create setKey(java.lang.String key) {
                  return (Create) super.setKey(key);
                }

                @Override
                public Create setOauthToken(java.lang.String oauthToken) {
                  return (Create) super.setOauthToken(oauthToken);
                }

                @Override
                public Create setPrettyPrint(java.lang.Boolean prettyPrint) {
                  return (Create) super.setPrettyPrint(prettyPrint);
                }

                @Override
                public Create setQuotaUser(java.lang.String quotaUser) {
                  return (Create) super.setQuotaUser(quotaUser);
                }

                @Override
                public Create setUploadType(java.lang.String uploadType) {
                  return (Create) super.setUploadType(uploadType);
                }

                @Override
                public Create setUploadProtocol(java.lang.String uploadProtocol) {
                  return (Create) super.setUploadProtocol(uploadProtocol);
                }

                /**
                 * Required. The resource name of the parent zone:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}`
                 */
                @com.google.api.client.util.Key private java.lang.String parent;

                /**
                 * Required. The resource name of the parent zone:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}`
                 */
                public java.lang.String getParent() {
                  return parent;
                }

                /**
                 * Required. The resource name of the parent zone:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}`
                 */
                public Create setParent(java.lang.String parent) {
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        PARENT_PATTERN.matcher(parent).matches(),
                        "Parameter parent must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                  }
                  this.parent = parent;
                  return this;
                }

                /**
                 * Optional. Only validate the request, but do not perform mutations. The default is
                 * false.
                 */
                @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

                /**
                 * Optional. Only validate the request, but do not perform mutations. The default is
                 * false.
                 */
                public java.lang.Boolean getValidateOnly() {
                  return validateOnly;
                }

                /**
                 * Optional. Only validate the request, but do not perform mutations. The default is
                 * false.
                 */
                public Create setValidateOnly(java.lang.Boolean validateOnly) {
                  this.validateOnly = validateOnly;
                  return this;
                }

                @Override
                public Create set(String parameterName, Object value) {
                  return (Create) super.set(parameterName, value);
                }
              }
              /**
               * Delete a metadata partition.
               *
               * <p>Create a request for the method "partitions.delete".
               *
               * <p>This request holds the parameters needed by the dataplex server. After setting
               * any optional parameters, call the {@link Delete#execute()} method to invoke the
               * remote operation.
               *
               * @param name Required. The resource name of the entity. format:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
               * @return the request
               */
              public Delete delete(java.lang.String name) throws java.io.IOException {
                Delete result = new Delete(name);
                initialize(result);
                return result;
              }

              public class Delete
                  extends CloudDataplexRequest<com.google.api.services.dataplex.v1.model.Empty> {

                private static final String REST_PATH = "v1/{+name}";

                private final java.util.regex.Pattern NAME_PATTERN =
                    java.util.regex.Pattern.compile(
                        "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");

                /**
                 * Delete a metadata partition.
                 *
                 * <p>Create a request for the method "partitions.delete".
                 *
                 * <p>This request holds the parameters needed by the the dataplex server. After
                 * setting any optional parameters, call the {@link Delete#execute()} method to
                 * invoke the remote operation.
                 *
                 * <p>{@link
                 * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
                 * must be called to initialize this instance immediately after invoking the
                 * constructor.
                 *
                 * @param name Required. The resource name of the entity. format:
                 *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 *     /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 * @since 1.13
                 */
                protected Delete(java.lang.String name) {
                  super(
                      CloudDataplex.this,
                      "DELETE",
                      REST_PATH,
                      null,
                      com.google.api.services.dataplex.v1.model.Empty.class);
                  this.name =
                      com.google.api.client.util.Preconditions.checkNotNull(
                          name, "Required parameter name must be specified.");
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        NAME_PATTERN.matcher(name).matches(),
                        "Parameter name must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");
                  }
                }

                @Override
                public Delete set$Xgafv(java.lang.String $Xgafv) {
                  return (Delete) super.set$Xgafv($Xgafv);
                }

                @Override
                public Delete setAccessToken(java.lang.String accessToken) {
                  return (Delete) super.setAccessToken(accessToken);
                }

                @Override
                public Delete setAlt(java.lang.String alt) {
                  return (Delete) super.setAlt(alt);
                }

                @Override
                public Delete setCallback(java.lang.String callback) {
                  return (Delete) super.setCallback(callback);
                }

                @Override
                public Delete setFields(java.lang.String fields) {
                  return (Delete) super.setFields(fields);
                }

                @Override
                public Delete setKey(java.lang.String key) {
                  return (Delete) super.setKey(key);
                }

                @Override
                public Delete setOauthToken(java.lang.String oauthToken) {
                  return (Delete) super.setOauthToken(oauthToken);
                }

                @Override
                public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
                  return (Delete) super.setPrettyPrint(prettyPrint);
                }

                @Override
                public Delete setQuotaUser(java.lang.String quotaUser) {
                  return (Delete) super.setQuotaUser(quotaUser);
                }

                @Override
                public Delete setUploadType(java.lang.String uploadType) {
                  return (Delete) super.setUploadType(uploadType);
                }

                @Override
                public Delete setUploadProtocol(java.lang.String uploadProtocol) {
                  return (Delete) super.setUploadProtocol(uploadProtocol);
                }

                /**
                 * Required. The resource name of the entity. format:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 */
                @com.google.api.client.util.Key private java.lang.String name;

                /**
                 * Required. The resource name of the entity. format:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 */
                public java.lang.String getName() {
                  return name;
                }

                /**
                 * Required. The resource name of the entity. format:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 */
                public Delete setName(java.lang.String name) {
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        NAME_PATTERN.matcher(name).matches(),
                        "Parameter name must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");
                  }
                  this.name = name;
                  return this;
                }

                /**
                 * Optional. The etag associated with the partition if it was previously retrieved.
                 */
                @com.google.api.client.util.Key private java.lang.String etag;

                /**
                 * Optional. The etag associated with the partition if it was previously retrieved.
                 */
                public java.lang.String getEtag() {
                  return etag;
                }

                /**
                 * Optional. The etag associated with the partition if it was previously retrieved.
                 */
                public Delete setEtag(java.lang.String etag) {
                  this.etag = etag;
                  return this;
                }

                @Override
                public Delete set(String parameterName, Object value) {
                  return (Delete) super.set(parameterName, value);
                }
              }
              /**
               * Get a metadata partition of a given entity.
               *
               * <p>Create a request for the method "partitions.get".
               *
               * <p>This request holds the parameters needed by the dataplex server. After setting
               * any optional parameters, call the {@link Get#execute()} method to invoke the remote
               * operation.
               *
               * @param name Required. The resource name of the partition:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
               * @return the request
               */
              public Get get(java.lang.String name) throws java.io.IOException {
                Get result = new Get(name);
                initialize(result);
                return result;
              }

              public class Get
                  extends CloudDataplexRequest<
                      com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition> {

                private static final String REST_PATH = "v1/{+name}";

                private final java.util.regex.Pattern NAME_PATTERN =
                    java.util.regex.Pattern.compile(
                        "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");

                /**
                 * Get a metadata partition of a given entity.
                 *
                 * <p>Create a request for the method "partitions.get".
                 *
                 * <p>This request holds the parameters needed by the the dataplex server. After
                 * setting any optional parameters, call the {@link Get#execute()} method to invoke
                 * the remote operation.
                 *
                 * <p>{@link
                 * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
                 * must be called to initialize this instance immediately after invoking the
                 * constructor.
                 *
                 * @param name Required. The resource name of the partition:
                 *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 *     /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 * @since 1.13
                 */
                protected Get(java.lang.String name) {
                  super(
                      CloudDataplex.this,
                      "GET",
                      REST_PATH,
                      null,
                      com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition
                          .class);
                  this.name =
                      com.google.api.client.util.Preconditions.checkNotNull(
                          name, "Required parameter name must be specified.");
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        NAME_PATTERN.matcher(name).matches(),
                        "Parameter name must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");
                  }
                }

                @Override
                public com.google.api.client.http.HttpResponse executeUsingHead()
                    throws java.io.IOException {
                  return super.executeUsingHead();
                }

                @Override
                public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                    throws java.io.IOException {
                  return super.buildHttpRequestUsingHead();
                }

                @Override
                public Get set$Xgafv(java.lang.String $Xgafv) {
                  return (Get) super.set$Xgafv($Xgafv);
                }

                @Override
                public Get setAccessToken(java.lang.String accessToken) {
                  return (Get) super.setAccessToken(accessToken);
                }

                @Override
                public Get setAlt(java.lang.String alt) {
                  return (Get) super.setAlt(alt);
                }

                @Override
                public Get setCallback(java.lang.String callback) {
                  return (Get) super.setCallback(callback);
                }

                @Override
                public Get setFields(java.lang.String fields) {
                  return (Get) super.setFields(fields);
                }

                @Override
                public Get setKey(java.lang.String key) {
                  return (Get) super.setKey(key);
                }

                @Override
                public Get setOauthToken(java.lang.String oauthToken) {
                  return (Get) super.setOauthToken(oauthToken);
                }

                @Override
                public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
                  return (Get) super.setPrettyPrint(prettyPrint);
                }

                @Override
                public Get setQuotaUser(java.lang.String quotaUser) {
                  return (Get) super.setQuotaUser(quotaUser);
                }

                @Override
                public Get setUploadType(java.lang.String uploadType) {
                  return (Get) super.setUploadType(uploadType);
                }

                @Override
                public Get setUploadProtocol(java.lang.String uploadProtocol) {
                  return (Get) super.setUploadProtocol(uploadProtocol);
                }

                /**
                 * Required. The resource name of the partition:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 */
                @com.google.api.client.util.Key private java.lang.String name;

                /**
                 * Required. The resource name of the partition:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 */
                public java.lang.String getName() {
                  return name;
                }

                /**
                 * Required. The resource name of the partition:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}/partitions/{partition_id}
                 */
                public Get setName(java.lang.String name) {
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        NAME_PATTERN.matcher(name).matches(),
                        "Parameter name must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");
                  }
                  this.name = name;
                  return this;
                }

                @Override
                public Get set(String parameterName, Object value) {
                  return (Get) super.set(parameterName, value);
                }
              }
              /**
               * List metadata partitions of a given entity.
               *
               * <p>Create a request for the method "partitions.list".
               *
               * <p>This request holds the parameters needed by the dataplex server. After setting
               * any optional parameters, call the {@link List#execute()} method to invoke the
               * remote operation.
               *
               * @param parent Required. The resource name of the parent entity:
               *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
               *     /zones/{zone_id}/entities/{entity_id}`
               * @return the request
               */
              public List list(java.lang.String parent) throws java.io.IOException {
                List result = new List(parent);
                initialize(result);
                return result;
              }

              public class List
                  extends CloudDataplexRequest<
                      com.google.api.services.dataplex.v1.model
                          .GoogleCloudDataplexV1ListPartitionsResponse> {

                private static final String REST_PATH = "v1/{+parent}/partitions";

                private final java.util.regex.Pattern PARENT_PATTERN =
                    java.util.regex.Pattern.compile(
                        "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");

                /**
                 * List metadata partitions of a given entity.
                 *
                 * <p>Create a request for the method "partitions.list".
                 *
                 * <p>This request holds the parameters needed by the the dataplex server. After
                 * setting any optional parameters, call the {@link List#execute()} method to invoke
                 * the remote operation.
                 *
                 * <p>{@link
                 * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
                 * must be called to initialize this instance immediately after invoking the
                 * constructor.
                 *
                 * @param parent Required. The resource name of the parent entity:
                 *     projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 *     /zones/{zone_id}/entities/{entity_id}`
                 * @since 1.13
                 */
                protected List(java.lang.String parent) {
                  super(
                      CloudDataplex.this,
                      "GET",
                      REST_PATH,
                      null,
                      com.google.api.services.dataplex.v1.model
                          .GoogleCloudDataplexV1ListPartitionsResponse.class);
                  this.parent =
                      com.google.api.client.util.Preconditions.checkNotNull(
                          parent, "Required parameter parent must be specified.");
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        PARENT_PATTERN.matcher(parent).matches(),
                        "Parameter parent must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                  }
                }

                @Override
                public com.google.api.client.http.HttpResponse executeUsingHead()
                    throws java.io.IOException {
                  return super.executeUsingHead();
                }

                @Override
                public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
                    throws java.io.IOException {
                  return super.buildHttpRequestUsingHead();
                }

                @Override
                public List set$Xgafv(java.lang.String $Xgafv) {
                  return (List) super.set$Xgafv($Xgafv);
                }

                @Override
                public List setAccessToken(java.lang.String accessToken) {
                  return (List) super.setAccessToken(accessToken);
                }

                @Override
                public List setAlt(java.lang.String alt) {
                  return (List) super.setAlt(alt);
                }

                @Override
                public List setCallback(java.lang.String callback) {
                  return (List) super.setCallback(callback);
                }

                @Override
                public List setFields(java.lang.String fields) {
                  return (List) super.setFields(fields);
                }

                @Override
                public List setKey(java.lang.String key) {
                  return (List) super.setKey(key);
                }

                @Override
                public List setOauthToken(java.lang.String oauthToken) {
                  return (List) super.setOauthToken(oauthToken);
                }

                @Override
                public List setPrettyPrint(java.lang.Boolean prettyPrint) {
                  return (List) super.setPrettyPrint(prettyPrint);
                }

                @Override
                public List setQuotaUser(java.lang.String quotaUser) {
                  return (List) super.setQuotaUser(quotaUser);
                }

                @Override
                public List setUploadType(java.lang.String uploadType) {
                  return (List) super.setUploadType(uploadType);
                }

                @Override
                public List setUploadProtocol(java.lang.String uploadProtocol) {
                  return (List) super.setUploadProtocol(uploadProtocol);
                }

                /**
                 * Required. The resource name of the parent entity:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}`
                 */
                @com.google.api.client.util.Key private java.lang.String parent;

                /**
                 * Required. The resource name of the parent entity:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}`
                 */
                public java.lang.String getParent() {
                  return parent;
                }

                /**
                 * Required. The resource name of the parent entity:
                 * projects/{project_number}/locations/{location_id}/lakes/{lake_id}
                 * /zones/{zone_id}/entities/{entity_id}`
                 */
                public List setParent(java.lang.String parent) {
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        PARENT_PATTERN.matcher(parent).matches(),
                        "Parameter parent must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+$");
                  }
                  this.parent = parent;
                  return this;
                }

                /** Optional. Filter request. */
                @com.google.api.client.util.Key private java.lang.String filter;

                /** Optional. Filter request. */
                public java.lang.String getFilter() {
                  return filter;
                }

                /** Optional. Filter request. */
                public List setFilter(java.lang.String filter) {
                  this.filter = filter;
                  return this;
                }

                /**
                 * Optional. Maximum number of partitions to return. The service may return fewer
                 * than this value. If unspecified, at most 10 partitions will be returned. The
                 * maximum value is 1000; values above 1000 will be coerced to 1000.
                 */
                @com.google.api.client.util.Key private java.lang.Integer pageSize;

                /**
                 * Optional. Maximum number of partitions to return. The service may return fewer
                 * than this value. If unspecified, at most 10 partitions will be returned. The
                 * maximum value is 1000; values above 1000 will be coerced to 1000.
                 */
                public java.lang.Integer getPageSize() {
                  return pageSize;
                }

                /**
                 * Optional. Maximum number of partitions to return. The service may return fewer
                 * than this value. If unspecified, at most 10 partitions will be returned. The
                 * maximum value is 1000; values above 1000 will be coerced to 1000.
                 */
                public List setPageSize(java.lang.Integer pageSize) {
                  this.pageSize = pageSize;
                  return this;
                }

                /**
                 * Optional. Page token received from a previous ListPartitions call. Provide this
                 * to retrieve the subsequent page. When paginating, all other parameters provided
                 * to ListPartitions must match the call that provided the page token.
                 */
                @com.google.api.client.util.Key private java.lang.String pageToken;

                /**
                 * Optional. Page token received from a previous ListPartitions call. Provide this
                 * to retrieve the subsequent page. When paginating, all other parameters provided
                 * to ListPartitions must match the call that provided the page token.
                 */
                public java.lang.String getPageToken() {
                  return pageToken;
                }

                /**
                 * Optional. Page token received from a previous ListPartitions call. Provide this
                 * to retrieve the subsequent page. When paginating, all other parameters provided
                 * to ListPartitions must match the call that provided the page token.
                 */
                public List setPageToken(java.lang.String pageToken) {
                  this.pageToken = pageToken;
                  return this;
                }

                @Override
                public List set(String parameterName, Object value) {
                  return (List) super.set(parameterName, value);
                }
              }
              /**
               * Update a metadata partition. Only supports full resource update.
               *
               * <p>Create a request for the method "partitions.update".
               *
               * <p>This request holds the parameters needed by the dataplex server. After setting
               * any optional parameters, call the {@link Update#execute()} method to invoke the
               * remote operation.
               *
               * @param name Output only. The resource name of the entity, of the form:
               *     projects/{project_number}/locations/{loca
               *     tion_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/partitions/{partition}
               *     The {partition} is a generated unique ID.
               * @param content the {@link
               *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition}
               * @return the request
               */
              public Update update(
                  java.lang.String name,
                  com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition content)
                  throws java.io.IOException {
                Update result = new Update(name, content);
                initialize(result);
                return result;
              }

              public class Update
                  extends CloudDataplexRequest<
                      com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition> {

                private static final String REST_PATH = "v1/{+name}";

                private final java.util.regex.Pattern NAME_PATTERN =
                    java.util.regex.Pattern.compile(
                        "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");

                /**
                 * Update a metadata partition. Only supports full resource update.
                 *
                 * <p>Create a request for the method "partitions.update".
                 *
                 * <p>This request holds the parameters needed by the the dataplex server. After
                 * setting any optional parameters, call the {@link Update#execute()} method to
                 * invoke the remote operation.
                 *
                 * <p>{@link
                 * Update#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
                 * must be called to initialize this instance immediately after invoking the
                 * constructor.
                 *
                 * @param name Output only. The resource name of the entity, of the form:
                 *     projects/{project_number}/locations/{loca
                 *     tion_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/partitions/{partition}
                 *     The {partition} is a generated unique ID.
                 * @param content the {@link
                 *     com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition}
                 * @since 1.13
                 */
                protected Update(
                    java.lang.String name,
                    com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition
                        content) {
                  super(
                      CloudDataplex.this,
                      "PUT",
                      REST_PATH,
                      content,
                      com.google.api.services.dataplex.v1.model.GoogleCloudDataplexV1Partition
                          .class);
                  this.name =
                      com.google.api.client.util.Preconditions.checkNotNull(
                          name, "Required parameter name must be specified.");
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        NAME_PATTERN.matcher(name).matches(),
                        "Parameter name must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");
                  }
                }

                @Override
                public Update set$Xgafv(java.lang.String $Xgafv) {
                  return (Update) super.set$Xgafv($Xgafv);
                }

                @Override
                public Update setAccessToken(java.lang.String accessToken) {
                  return (Update) super.setAccessToken(accessToken);
                }

                @Override
                public Update setAlt(java.lang.String alt) {
                  return (Update) super.setAlt(alt);
                }

                @Override
                public Update setCallback(java.lang.String callback) {
                  return (Update) super.setCallback(callback);
                }

                @Override
                public Update setFields(java.lang.String fields) {
                  return (Update) super.setFields(fields);
                }

                @Override
                public Update setKey(java.lang.String key) {
                  return (Update) super.setKey(key);
                }

                @Override
                public Update setOauthToken(java.lang.String oauthToken) {
                  return (Update) super.setOauthToken(oauthToken);
                }

                @Override
                public Update setPrettyPrint(java.lang.Boolean prettyPrint) {
                  return (Update) super.setPrettyPrint(prettyPrint);
                }

                @Override
                public Update setQuotaUser(java.lang.String quotaUser) {
                  return (Update) super.setQuotaUser(quotaUser);
                }

                @Override
                public Update setUploadType(java.lang.String uploadType) {
                  return (Update) super.setUploadType(uploadType);
                }

                @Override
                public Update setUploadProtocol(java.lang.String uploadProtocol) {
                  return (Update) super.setUploadProtocol(uploadProtocol);
                }

                /**
                 * Output only. The resource name of the entity, of the form: projects/{project_numb
                 * er}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/par
                 * titions/{partition} The {partition} is a generated unique ID.
                 */
                @com.google.api.client.util.Key private java.lang.String name;

                /**
                 * Output only. The resource name of the entity, of the form:
                 * projects/{project_number}/locations/{loc
                 * ation_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/partitions/{partition}
                 * The {partition} is a generated unique ID.
                 */
                public java.lang.String getName() {
                  return name;
                }

                /**
                 * Output only. The resource name of the entity, of the form: projects/{project_numb
                 * er}/locations/{location_id}/lakes/{lake_id}/zones/{zone_id}/entities/{entity}/par
                 * titions/{partition} The {partition} is a generated unique ID.
                 */
                public Update setName(java.lang.String name) {
                  if (!getSuppressPatternChecks()) {
                    com.google.api.client.util.Preconditions.checkArgument(
                        NAME_PATTERN.matcher(name).matches(),
                        "Parameter name must conform to the pattern "
                            + "^projects/[^/]+/locations/[^/]+/lakes/[^/]+/zones/[^/]+/entities/[^/]+/partitions/[^/]+$");
                  }
                  this.name = name;
                  return this;
                }

                /** Required. Mask of fields to update. */
                @com.google.api.client.util.Key private String updateMask;

                /** Required. Mask of fields to update. */
                public String getUpdateMask() {
                  return updateMask;
                }

                /** Required. Mask of fields to update. */
                public Update setUpdateMask(String updateMask) {
                  this.updateMask = updateMask;
                  return this;
                }

                /**
                 * Optional. Only validate the request, but do not perform mutations. The default is
                 * false.
                 */
                @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

                /**
                 * Optional. Only validate the request, but do not perform mutations. The default is
                 * false.
                 */
                public java.lang.Boolean getValidateOnly() {
                  return validateOnly;
                }

                /**
                 * Optional. Only validate the request, but do not perform mutations. The default is
                 * false.
                 */
                public Update setValidateOnly(java.lang.Boolean validateOnly) {
                  this.validateOnly = validateOnly;
                  return this;
                }

                @Override
                public Update set(String parameterName, Object value) {
                  return (Update) super.set(parameterName, value);
                }
              }
            }
          }
        }
      }
      /**
       * An accessor for creating requests from the Operations collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code CloudDataplex dataplex = new CloudDataplex(...);}
       *   {@code CloudDataplex.Operations.List request = dataplex.operations().list(parameters ...)}
       * </pre>
       *
       * @return the resource collection
       */
      public Operations operations() {
        return new Operations();
      }

      /** The "operations" collection of methods. */
      public class Operations {

        /**
         * Starts asynchronous cancellation on a long-running operation. The server makes a best
         * effort to cancel the operation, but success is not guaranteed. If the server doesn't
         * support this method, it returns google.rpc.Code.UNIMPLEMENTED. Clients can use
         * Operations.GetOperation or other methods to check whether the cancellation succeeded or
         * whether the operation completed despite cancellation. On successful cancellation, the
         * operation is not deleted; instead, it becomes an operation with an Operation.error value
         * with a google.rpc.Status.code of 1, corresponding to Code.CANCELLED.
         *
         * <p>Create a request for the method "operations.cancel".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Cancel#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the operation resource to be cancelled.
         * @param content the {@link
         *     com.google.api.services.dataplex.v1.model.GoogleLongrunningCancelOperationRequest}
         * @return the request
         */
        public Cancel cancel(
            java.lang.String name,
            com.google.api.services.dataplex.v1.model.GoogleLongrunningCancelOperationRequest
                content)
            throws java.io.IOException {
          Cancel result = new Cancel(name, content);
          initialize(result);
          return result;
        }

        public class Cancel
            extends CloudDataplexRequest<com.google.api.services.dataplex.v1.model.Empty> {

          private static final String REST_PATH = "v1/{+name}:cancel";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

          /**
           * Starts asynchronous cancellation on a long-running operation. The server makes a best
           * effort to cancel the operation, but success is not guaranteed. If the server doesn't
           * support this method, it returns google.rpc.Code.UNIMPLEMENTED. Clients can use
           * Operations.GetOperation or other methods to check whether the cancellation succeeded or
           * whether the operation completed despite cancellation. On successful cancellation, the
           * operation is not deleted; instead, it becomes an operation with an Operation.error
           * value with a google.rpc.Status.code of 1, corresponding to Code.CANCELLED.
           *
           * <p>Create a request for the method "operations.cancel".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Cancel#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Cancel#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource to be cancelled.
           * @param content the {@link
           *     com.google.api.services.dataplex.v1.model.GoogleLongrunningCancelOperationRequest}
           * @since 1.13
           */
          protected Cancel(
              java.lang.String name,
              com.google.api.services.dataplex.v1.model.GoogleLongrunningCancelOperationRequest
                  content) {
            super(
                CloudDataplex.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.dataplex.v1.model.Empty.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/operations/[^/]+$");
            }
          }

          @Override
          public Cancel set$Xgafv(java.lang.String $Xgafv) {
            return (Cancel) super.set$Xgafv($Xgafv);
          }

          @Override
          public Cancel setAccessToken(java.lang.String accessToken) {
            return (Cancel) super.setAccessToken(accessToken);
          }

          @Override
          public Cancel setAlt(java.lang.String alt) {
            return (Cancel) super.setAlt(alt);
          }

          @Override
          public Cancel setCallback(java.lang.String callback) {
            return (Cancel) super.setCallback(callback);
          }

          @Override
          public Cancel setFields(java.lang.String fields) {
            return (Cancel) super.setFields(fields);
          }

          @Override
          public Cancel setKey(java.lang.String key) {
            return (Cancel) super.setKey(key);
          }

          @Override
          public Cancel setOauthToken(java.lang.String oauthToken) {
            return (Cancel) super.setOauthToken(oauthToken);
          }

          @Override
          public Cancel setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Cancel) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Cancel setQuotaUser(java.lang.String quotaUser) {
            return (Cancel) super.setQuotaUser(quotaUser);
          }

          @Override
          public Cancel setUploadType(java.lang.String uploadType) {
            return (Cancel) super.setUploadType(uploadType);
          }

          @Override
          public Cancel setUploadProtocol(java.lang.String uploadProtocol) {
            return (Cancel) super.setUploadProtocol(uploadProtocol);
          }

          /** The name of the operation resource to be cancelled. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the operation resource to be cancelled. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the operation resource to be cancelled. */
          public Cancel setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/operations/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Cancel set(String parameterName, Object value) {
            return (Cancel) super.set(parameterName, value);
          }
        }
        /**
         * Deletes a long-running operation. This method indicates that the client is no longer
         * interested in the operation result. It does not cancel the operation. If the server
         * doesn't support this method, it returns google.rpc.Code.UNIMPLEMENTED.
         *
         * <p>Create a request for the method "operations.delete".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the operation resource to be deleted.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends CloudDataplexRequest<com.google.api.services.dataplex.v1.model.Empty> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

          /**
           * Deletes a long-running operation. This method indicates that the client is no longer
           * interested in the operation result. It does not cancel the operation. If the server
           * doesn't support this method, it returns google.rpc.Code.UNIMPLEMENTED.
           *
           * <p>Create a request for the method "operations.delete".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource to be deleted.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                CloudDataplex.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.Empty.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/operations/[^/]+$");
            }
          }

          @Override
          public Delete set$Xgafv(java.lang.String $Xgafv) {
            return (Delete) super.set$Xgafv($Xgafv);
          }

          @Override
          public Delete setAccessToken(java.lang.String accessToken) {
            return (Delete) super.setAccessToken(accessToken);
          }

          @Override
          public Delete setAlt(java.lang.String alt) {
            return (Delete) super.setAlt(alt);
          }

          @Override
          public Delete setCallback(java.lang.String callback) {
            return (Delete) super.setCallback(callback);
          }

          @Override
          public Delete setFields(java.lang.String fields) {
            return (Delete) super.setFields(fields);
          }

          @Override
          public Delete setKey(java.lang.String key) {
            return (Delete) super.setKey(key);
          }

          @Override
          public Delete setOauthToken(java.lang.String oauthToken) {
            return (Delete) super.setOauthToken(oauthToken);
          }

          @Override
          public Delete setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Delete) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Delete setQuotaUser(java.lang.String quotaUser) {
            return (Delete) super.setQuotaUser(quotaUser);
          }

          @Override
          public Delete setUploadType(java.lang.String uploadType) {
            return (Delete) super.setUploadType(uploadType);
          }

          @Override
          public Delete setUploadProtocol(java.lang.String uploadProtocol) {
            return (Delete) super.setUploadProtocol(uploadProtocol);
          }

          /** The name of the operation resource to be deleted. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the operation resource to be deleted. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the operation resource to be deleted. */
          public Delete setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/operations/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Delete set(String parameterName, Object value) {
            return (Delete) super.set(parameterName, value);
          }
        }
        /**
         * Gets the latest state of a long-running operation. Clients can use this method to poll
         * the operation result at intervals as recommended by the API service.
         *
         * <p>Create a request for the method "operations.get".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the operation resource.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

          /**
           * Gets the latest state of a long-running operation. Clients can use this method to poll
           * the operation result at intervals as recommended by the API service.
           *
           * <p>Create a request for the method "operations.get".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                CloudDataplex.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.GoogleLongrunningOperation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/operations/[^/]+$");
            }
          }

          @Override
          public com.google.api.client.http.HttpResponse executeUsingHead()
              throws java.io.IOException {
            return super.executeUsingHead();
          }

          @Override
          public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
              throws java.io.IOException {
            return super.buildHttpRequestUsingHead();
          }

          @Override
          public Get set$Xgafv(java.lang.String $Xgafv) {
            return (Get) super.set$Xgafv($Xgafv);
          }

          @Override
          public Get setAccessToken(java.lang.String accessToken) {
            return (Get) super.setAccessToken(accessToken);
          }

          @Override
          public Get setAlt(java.lang.String alt) {
            return (Get) super.setAlt(alt);
          }

          @Override
          public Get setCallback(java.lang.String callback) {
            return (Get) super.setCallback(callback);
          }

          @Override
          public Get setFields(java.lang.String fields) {
            return (Get) super.setFields(fields);
          }

          @Override
          public Get setKey(java.lang.String key) {
            return (Get) super.setKey(key);
          }

          @Override
          public Get setOauthToken(java.lang.String oauthToken) {
            return (Get) super.setOauthToken(oauthToken);
          }

          @Override
          public Get setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Get) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Get setQuotaUser(java.lang.String quotaUser) {
            return (Get) super.setQuotaUser(quotaUser);
          }

          @Override
          public Get setUploadType(java.lang.String uploadType) {
            return (Get) super.setUploadType(uploadType);
          }

          @Override
          public Get setUploadProtocol(java.lang.String uploadProtocol) {
            return (Get) super.setUploadProtocol(uploadProtocol);
          }

          /** The name of the operation resource. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the operation resource. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the operation resource. */
          public Get setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/operations/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Get set(String parameterName, Object value) {
            return (Get) super.set(parameterName, value);
          }
        }
        /**
         * Lists operations that match the specified filter in the request. If the server doesn't
         * support this method, it returns UNIMPLEMENTED.NOTE: the name binding allows API services
         * to override the binding to use different resource name schemes, such as users/operations.
         * To override the binding, API services can add a binding such as
         * "/v1/{name=users}/operations" to their service configuration. For backwards
         * compatibility, the default name includes the operations collection id, however overriding
         * users must ensure the name binding is the parent resource, without the operations
         * collection id.
         *
         * <p>Create a request for the method "operations.list".
         *
         * <p>This request holds the parameters needed by the dataplex server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the operation's parent resource.
         * @return the request
         */
        public List list(java.lang.String name) throws java.io.IOException {
          List result = new List(name);
          initialize(result);
          return result;
        }

        public class List
            extends CloudDataplexRequest<
                com.google.api.services.dataplex.v1.model.GoogleLongrunningListOperationsResponse> {

          private static final String REST_PATH = "v1/{+name}/operations";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Lists operations that match the specified filter in the request. If the server doesn't
           * support this method, it returns UNIMPLEMENTED.NOTE: the name binding allows API
           * services to override the binding to use different resource name schemes, such as
           * users/operations. To override the binding, API services can add a binding such as
           * "/v1/{name=users}/operations" to their service configuration. For backwards
           * compatibility, the default name includes the operations collection id, however
           * overriding users must ensure the name binding is the parent resource, without the
           * operations collection id.
           *
           * <p>Create a request for the method "operations.list".
           *
           * <p>This request holds the parameters needed by the the dataplex server. After setting
           * any optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation's parent resource.
           * @since 1.13
           */
          protected List(java.lang.String name) {
            super(
                CloudDataplex.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.dataplex.v1.model.GoogleLongrunningListOperationsResponse
                    .class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
          }

          @Override
          public com.google.api.client.http.HttpResponse executeUsingHead()
              throws java.io.IOException {
            return super.executeUsingHead();
          }

          @Override
          public com.google.api.client.http.HttpRequest buildHttpRequestUsingHead()
              throws java.io.IOException {
            return super.buildHttpRequestUsingHead();
          }

          @Override
          public List set$Xgafv(java.lang.String $Xgafv) {
            return (List) super.set$Xgafv($Xgafv);
          }

          @Override
          public List setAccessToken(java.lang.String accessToken) {
            return (List) super.setAccessToken(accessToken);
          }

          @Override
          public List setAlt(java.lang.String alt) {
            return (List) super.setAlt(alt);
          }

          @Override
          public List setCallback(java.lang.String callback) {
            return (List) super.setCallback(callback);
          }

          @Override
          public List setFields(java.lang.String fields) {
            return (List) super.setFields(fields);
          }

          @Override
          public List setKey(java.lang.String key) {
            return (List) super.setKey(key);
          }

          @Override
          public List setOauthToken(java.lang.String oauthToken) {
            return (List) super.setOauthToken(oauthToken);
          }

          @Override
          public List setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (List) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public List setQuotaUser(java.lang.String quotaUser) {
            return (List) super.setQuotaUser(quotaUser);
          }

          @Override
          public List setUploadType(java.lang.String uploadType) {
            return (List) super.setUploadType(uploadType);
          }

          @Override
          public List setUploadProtocol(java.lang.String uploadProtocol) {
            return (List) super.setUploadProtocol(uploadProtocol);
          }

          /** The name of the operation's parent resource. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the operation's parent resource. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the operation's parent resource. */
          public List setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /** The standard list filter. */
          @com.google.api.client.util.Key private java.lang.String filter;

          /** The standard list filter. */
          public java.lang.String getFilter() {
            return filter;
          }

          /** The standard list filter. */
          public List setFilter(java.lang.String filter) {
            this.filter = filter;
            return this;
          }

          /** The standard list page size. */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /** The standard list page size. */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /** The standard list page size. */
          public List setPageSize(java.lang.Integer pageSize) {
            this.pageSize = pageSize;
            return this;
          }

          /** The standard list page token. */
          @com.google.api.client.util.Key private java.lang.String pageToken;

          /** The standard list page token. */
          public java.lang.String getPageToken() {
            return pageToken;
          }

          /** The standard list page token. */
          public List setPageToken(java.lang.String pageToken) {
            this.pageToken = pageToken;
            return this;
          }

          @Override
          public List set(String parameterName, Object value) {
            return (List) super.set(parameterName, value);
          }
        }
      }
    }
  }

  /**
   * Builder for {@link CloudDataplex}.
   *
   * <p>Implementation is not thread-safe.
   *
   * @since 1.3.0
   */
  public static final class Builder
      extends com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient.Builder {

    private static String chooseEndpoint(com.google.api.client.http.HttpTransport transport) {
      // If the GOOGLE_API_USE_MTLS_ENDPOINT environment variable value is "always", use mTLS
      // endpoint.
      // If the env variable is "auto", use mTLS endpoint if and only if the transport is mTLS.
      // Use the regular endpoint for all other cases.
      String useMtlsEndpoint = System.getenv("GOOGLE_API_USE_MTLS_ENDPOINT");
      useMtlsEndpoint = useMtlsEndpoint == null ? "auto" : useMtlsEndpoint;
      if ("always".equals(useMtlsEndpoint)
          || ("auto".equals(useMtlsEndpoint) && transport != null && transport.isMtls())) {
        return DEFAULT_MTLS_ROOT_URL;
      }
      return DEFAULT_ROOT_URL;
    }

    /**
     * Returns an instance of a new builder.
     *
     * @param transport HTTP transport, which should normally be:
     *     <ul>
     *       <li>Google App Engine: {@code
     *           com.google.api.client.extensions.appengine.http.UrlFetchTransport}
     *       <li>Android: {@code newCompatibleTransport} from {@code
     *           com.google.api.client.extensions.android.http.AndroidHttp}
     *       <li>Java: {@link
     *           com.google.api.client.googleapis.javanet.GoogleNetHttpTransport#newTrustedTransport()}
     *     </ul>
     *
     * @param jsonFactory JSON factory, which may be:
     *     <ul>
     *       <li>Jackson: {@code com.google.api.client.json.jackson2.JacksonFactory}
     *       <li>Google GSON: {@code com.google.api.client.json.gson.GsonFactory}
     *       <li>Android Honeycomb or higher: {@code
     *           com.google.api.client.extensions.android.json.AndroidJsonFactory}
     *     </ul>
     *
     * @param httpRequestInitializer HTTP request initializer or {@code null} for none
     * @since 1.7
     */
    public Builder(
        com.google.api.client.http.HttpTransport transport,
        com.google.api.client.json.JsonFactory jsonFactory,
        com.google.api.client.http.HttpRequestInitializer httpRequestInitializer) {
      super(
          transport,
          jsonFactory,
          Builder.chooseEndpoint(transport),
          DEFAULT_SERVICE_PATH,
          httpRequestInitializer,
          false);
      setBatchPath(DEFAULT_BATCH_PATH);
    }

    /** Builds a new instance of {@link CloudDataplex}. */
    @Override
    public CloudDataplex build() {
      return new CloudDataplex(this);
    }

    @Override
    public Builder setRootUrl(String rootUrl) {
      return (Builder) super.setRootUrl(rootUrl);
    }

    @Override
    public Builder setServicePath(String servicePath) {
      return (Builder) super.setServicePath(servicePath);
    }

    @Override
    public Builder setBatchPath(String batchPath) {
      return (Builder) super.setBatchPath(batchPath);
    }

    @Override
    public Builder setHttpRequestInitializer(
        com.google.api.client.http.HttpRequestInitializer httpRequestInitializer) {
      return (Builder) super.setHttpRequestInitializer(httpRequestInitializer);
    }

    @Override
    public Builder setApplicationName(String applicationName) {
      return (Builder) super.setApplicationName(applicationName);
    }

    @Override
    public Builder setSuppressPatternChecks(boolean suppressPatternChecks) {
      return (Builder) super.setSuppressPatternChecks(suppressPatternChecks);
    }

    @Override
    public Builder setSuppressRequiredParameterChecks(boolean suppressRequiredParameterChecks) {
      return (Builder) super.setSuppressRequiredParameterChecks(suppressRequiredParameterChecks);
    }

    @Override
    public Builder setSuppressAllChecks(boolean suppressAllChecks) {
      return (Builder) super.setSuppressAllChecks(suppressAllChecks);
    }

    /**
     * Set the {@link CloudDataplexRequestInitializer}.
     *
     * @since 1.12
     */
    public Builder setCloudDataplexRequestInitializer(
        CloudDataplexRequestInitializer clouddataplexRequestInitializer) {
      return (Builder) super.setGoogleClientRequestInitializer(clouddataplexRequestInitializer);
    }

    @Override
    public Builder setGoogleClientRequestInitializer(
        com.google.api.client.googleapis.services.GoogleClientRequestInitializer
            googleClientRequestInitializer) {
      return (Builder) super.setGoogleClientRequestInitializer(googleClientRequestInitializer);
    }
  }
}

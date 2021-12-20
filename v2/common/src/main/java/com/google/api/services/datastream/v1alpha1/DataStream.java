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
package com.google.api.services.datastream.v1alpha1;

/**
 * Service definition for DataStream (v1alpha1).
 *
 * <p>
 *
 * <p>For more information about this service, see the <a
 * href="https://cloud.google.com/datastream/" target="_blank">API Documentation</a>
 *
 * <p>This service uses {@link DataStreamRequestInitializer} to initialize global parameters via its
 * {@link Builder}.
 *
 * @since 1.3
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public class DataStream
    extends com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient {

  // Note: Leave this static initializer at the top of the file.
  static {
    com.google.api.client.util.Preconditions.checkState(
        com.google.api.client.googleapis.GoogleUtils.MAJOR_VERSION == 1
            && com.google.api.client.googleapis.GoogleUtils.MINOR_VERSION >= 15,
        "You are currently running with version %s of google-api-client. "
            + "You need at least version 1.15 of google-api-client to run version "
            + "1.25.0-SNAPSHOT of the DataStream API library.",
        com.google.api.client.googleapis.GoogleUtils.VERSION);
  }

  /**
   * The default encoded root URL of the service. This is determined when the library is generated
   * and normally should not be changed.
   *
   * @since 1.7
   */
  public static final String DEFAULT_ROOT_URL = "https://datastream.googleapis.com/";

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
  public DataStream(
      com.google.api.client.http.HttpTransport transport,
      com.google.api.client.json.JsonFactory jsonFactory,
      com.google.api.client.http.HttpRequestInitializer httpRequestInitializer) {
    this(new Builder(transport, jsonFactory, httpRequestInitializer));
  }

  /** @param builder builder */
  DataStream(Builder builder) {
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
   *   {@code DataStream datastream = new DataStream(...);}
   *   {@code DataStream.Projects.List request = datastream.projects().list(parameters ...)}
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
     *   {@code DataStream datastream = new DataStream(...);}
     *   {@code DataStream.Locations.List request = datastream.locations().list(parameters ...)}
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
       * The FetchStaticIps API call exposes the static ips used by Datastream. Typically, a request
       * returns children data objects under a parent data object that’s optionally supplied in the
       * request.
       *
       * <p>Create a request for the method "locations.fetchStaticIps".
       *
       * <p>This request holds the parameters needed by the datastream server. After setting any
       * optional parameters, call the {@link FetchStaticIps#execute()} method to invoke the remote
       * operation.
       *
       * @param name Required. The name resource of the Response type. Must be in the format
       *     `projects/locations`.
       * @return the request
       */
      public FetchStaticIps fetchStaticIps(java.lang.String name) throws java.io.IOException {
        FetchStaticIps result = new FetchStaticIps(name);
        initialize(result);
        return result;
      }

      public class FetchStaticIps
          extends DataStreamRequest<
              com.google.api.services.datastream.v1alpha1.model.FetchStaticIpsResponse> {

        private static final String REST_PATH = "v1alpha1/{+name}:fetchStaticIps";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

        /**
         * The FetchStaticIps API call exposes the static ips used by Datastream. Typically, a
         * request returns children data objects under a parent data object that’s optionally
         * supplied in the request.
         *
         * <p>Create a request for the method "locations.fetchStaticIps".
         *
         * <p>This request holds the parameters needed by the the datastream server. After setting
         * any optional parameters, call the {@link FetchStaticIps#execute()} method to invoke the
         * remote operation.
         *
         * <p>{@link FetchStaticIps#initialize(com.google.api.client.googleapis.services.Abstr
         * actGoogleClientRequest)} must be called to initialize this instance immediately after
         * invoking the constructor.
         *
         * @param name Required. The name resource of the Response type. Must be in the format
         *     `projects/locations`.
         * @since 1.13
         */
        protected FetchStaticIps(java.lang.String name) {
          super(
              DataStream.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.datastream.v1alpha1.model.FetchStaticIpsResponse.class);
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
        public FetchStaticIps set$Xgafv(java.lang.String $Xgafv) {
          return (FetchStaticIps) super.set$Xgafv($Xgafv);
        }

        @Override
        public FetchStaticIps setAccessToken(java.lang.String accessToken) {
          return (FetchStaticIps) super.setAccessToken(accessToken);
        }

        @Override
        public FetchStaticIps setAlt(java.lang.String alt) {
          return (FetchStaticIps) super.setAlt(alt);
        }

        @Override
        public FetchStaticIps setCallback(java.lang.String callback) {
          return (FetchStaticIps) super.setCallback(callback);
        }

        @Override
        public FetchStaticIps setFields(java.lang.String fields) {
          return (FetchStaticIps) super.setFields(fields);
        }

        @Override
        public FetchStaticIps setKey(java.lang.String key) {
          return (FetchStaticIps) super.setKey(key);
        }

        @Override
        public FetchStaticIps setOauthToken(java.lang.String oauthToken) {
          return (FetchStaticIps) super.setOauthToken(oauthToken);
        }

        @Override
        public FetchStaticIps setPrettyPrint(java.lang.Boolean prettyPrint) {
          return (FetchStaticIps) super.setPrettyPrint(prettyPrint);
        }

        @Override
        public FetchStaticIps setQuotaUser(java.lang.String quotaUser) {
          return (FetchStaticIps) super.setQuotaUser(quotaUser);
        }

        @Override
        public FetchStaticIps setUploadType(java.lang.String uploadType) {
          return (FetchStaticIps) super.setUploadType(uploadType);
        }

        @Override
        public FetchStaticIps setUploadProtocol(java.lang.String uploadProtocol) {
          return (FetchStaticIps) super.setUploadProtocol(uploadProtocol);
        }

        /**
         * Required. The name resource of the Response type. Must be in the format
         * `projects/locations`.
         */
        @com.google.api.client.util.Key private java.lang.String name;

        /**
         * Required. The name resource of the Response type. Must be in the format
         * `projects/locations`.
         */
        public java.lang.String getName() {
          return name;
        }

        /**
         * Required. The name resource of the Response type. Must be in the format
         * `projects/locations`.
         */
        public FetchStaticIps setName(java.lang.String name) {
          if (!getSuppressPatternChecks()) {
            com.google.api.client.util.Preconditions.checkArgument(
                NAME_PATTERN.matcher(name).matches(),
                "Parameter name must conform to the pattern " + "^projects/[^/]+/locations/[^/]+$");
          }
          this.name = name;
          return this;
        }

        /** Maximum number of Ips to return, will likely not be specified. */
        @com.google.api.client.util.Key private java.lang.Integer pageSize;

        /** Maximum number of Ips to return, will likely not be specified. */
        public java.lang.Integer getPageSize() {
          return pageSize;
        }

        /** Maximum number of Ips to return, will likely not be specified. */
        public FetchStaticIps setPageSize(java.lang.Integer pageSize) {
          this.pageSize = pageSize;
          return this;
        }

        /**
         * A page token, received from a previous `ListStaticIps` call. will likely not be
         * specified.
         */
        @com.google.api.client.util.Key private java.lang.String pageToken;

        /**
         * A page token, received from a previous `ListStaticIps` call. will likely not be
         * specified.
         */
        public java.lang.String getPageToken() {
          return pageToken;
        }

        /**
         * A page token, received from a previous `ListStaticIps` call. will likely not be
         * specified.
         */
        public FetchStaticIps setPageToken(java.lang.String pageToken) {
          this.pageToken = pageToken;
          return this;
        }

        @Override
        public FetchStaticIps set(String parameterName, Object value) {
          return (FetchStaticIps) super.set(parameterName, value);
        }
      }
      /**
       * Gets information about a location.
       *
       * <p>Create a request for the method "locations.get".
       *
       * <p>This request holds the parameters needed by the datastream server. After setting any
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
          extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Location> {

        private static final String REST_PATH = "v1alpha1/{+name}";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

        /**
         * Gets information about a location.
         *
         * <p>Create a request for the method "locations.get".
         *
         * <p>This request holds the parameters needed by the the datastream server. After setting
         * any optional parameters, call the {@link Get#execute()} method to invoke the remote
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
              DataStream.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.datastream.v1alpha1.model.Location.class);
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
       * <p>This request holds the parameters needed by the datastream server. After setting any
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
          extends DataStreamRequest<
              com.google.api.services.datastream.v1alpha1.model.ListLocationsResponse> {

        private static final String REST_PATH = "v1alpha1/{+name}/locations";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+$");

        /**
         * Lists information about the supported locations for this service.
         *
         * <p>Create a request for the method "locations.list".
         *
         * <p>This request holds the parameters needed by the the datastream server. After setting
         * any optional parameters, call the {@link List#execute()} method to invoke the remote
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
              DataStream.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.datastream.v1alpha1.model.ListLocationsResponse.class);
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

      /**
       * An accessor for creating requests from the ConnectionProfiles collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code DataStream datastream = new DataStream(...);}
       *   {@code DataStream.ConnectionProfiles.List request = datastream.connectionProfiles().list(parameters ...)}
       * </pre>
       *
       * @return the resource collection
       */
      public ConnectionProfiles connectionProfiles() {
        return new ConnectionProfiles();
      }

      /** The "connectionProfiles" collection of methods. */
      public class ConnectionProfiles {

        /**
         * Use this method to create a connection profile in a project and location.
         *
         * <p>Create a request for the method "connectionProfiles.create".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Create#execute()} method to invoke the remote
         * operation.
         *
         * @param parent The parent that owns the collection of ConnectionProfiles.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.ConnectionProfile}
         * @return the request
         */
        public Create create(
            java.lang.String parent,
            com.google.api.services.datastream.v1alpha1.model.ConnectionProfile content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+parent}/connectionProfiles";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to create a connection profile in a project and location.
           *
           * <p>Create a request for the method "connectionProfiles.create".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent The parent that owns the collection of ConnectionProfiles.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.ConnectionProfile}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent,
              com.google.api.services.datastream.v1alpha1.model.ConnectionProfile content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
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

          /** The parent that owns the collection of ConnectionProfiles. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** The parent that owns the collection of ConnectionProfiles. */
          public java.lang.String getParent() {
            return parent;
          }

          /** The parent that owns the collection of ConnectionProfiles. */
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

          /** The Connection Profile identifier. */
          @com.google.api.client.util.Key private java.lang.String connectionProfileId;

          /** The Connection Profile identifier. */
          public java.lang.String getConnectionProfileId() {
            return connectionProfileId;
          }

          /** The Connection Profile identifier. */
          public Create setConnectionProfileId(java.lang.String connectionProfileId) {
            this.connectionProfileId = connectionProfileId;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Create setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          @Override
          public Create set(String parameterName, Object value) {
            return (Create) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to delete a connection profile..
         *
         * <p>Create a request for the method "connectionProfiles.delete".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the Connection Profile resource to delete.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");

          /**
           * Use this method to delete a connection profile..
           *
           * <p>Create a request for the method "connectionProfiles.delete".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the Connection Profile resource to delete.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                DataStream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");
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

          /** The name of the Connection Profile resource to delete. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the Connection Profile resource to delete. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the Connection Profile resource to delete. */
          public Delete setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Delete setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          @Override
          public Delete set(String parameterName, Object value) {
            return (Delete) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to discover a connection profile. The discover API call exposes the data
         * objects and metadata belonging to the profile. Typically, a request returns children data
         * objects under a parent data object that’s optionally supplied in the request.
         *
         * <p>Create a request for the method "connectionProfiles.discover".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Discover#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent resource of the ConnectionProfile type. Must be in the
         *     format `projects/locations`.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileRequest}
         * @return the request
         */
        public Discover discover(
            java.lang.String parent,
            com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileRequest
                content)
            throws java.io.IOException {
          Discover result = new Discover(parent, content);
          initialize(result);
          return result;
        }

        public class Discover
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model
                    .DiscoverConnectionProfileResponse> {

          private static final String REST_PATH = "v1alpha1/{+parent}/connectionProfiles:discover";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to discover a connection profile. The discover API call exposes the
           * data objects and metadata belonging to the profile. Typically, a request returns
           * children data objects under a parent data object that’s optionally supplied in the
           * request.
           *
           * <p>Create a request for the method "connectionProfiles.discover".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Discover#execute()} method to invoke the
           * remote operation.
           *
           * <p>{@link
           * Discover#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent resource of the ConnectionProfile type. Must be in
           *     the format `projects/locations`.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileRequest}
           * @since 1.13
           */
          protected Discover(
              java.lang.String parent,
              com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileRequest
                  content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.DiscoverConnectionProfileResponse
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
          public Discover set$Xgafv(java.lang.String $Xgafv) {
            return (Discover) super.set$Xgafv($Xgafv);
          }

          @Override
          public Discover setAccessToken(java.lang.String accessToken) {
            return (Discover) super.setAccessToken(accessToken);
          }

          @Override
          public Discover setAlt(java.lang.String alt) {
            return (Discover) super.setAlt(alt);
          }

          @Override
          public Discover setCallback(java.lang.String callback) {
            return (Discover) super.setCallback(callback);
          }

          @Override
          public Discover setFields(java.lang.String fields) {
            return (Discover) super.setFields(fields);
          }

          @Override
          public Discover setKey(java.lang.String key) {
            return (Discover) super.setKey(key);
          }

          @Override
          public Discover setOauthToken(java.lang.String oauthToken) {
            return (Discover) super.setOauthToken(oauthToken);
          }

          @Override
          public Discover setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Discover) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Discover setQuotaUser(java.lang.String quotaUser) {
            return (Discover) super.setQuotaUser(quotaUser);
          }

          @Override
          public Discover setUploadType(java.lang.String uploadType) {
            return (Discover) super.setUploadType(uploadType);
          }

          @Override
          public Discover setUploadProtocol(java.lang.String uploadProtocol) {
            return (Discover) super.setUploadProtocol(uploadProtocol);
          }

          /**
           * Required. The parent resource of the ConnectionProfile type. Must be in the format
           * `projects/locations`.
           */
          @com.google.api.client.util.Key private java.lang.String parent;

          /**
           * Required. The parent resource of the ConnectionProfile type. Must be in the format
           * `projects/locations`.
           */
          public java.lang.String getParent() {
            return parent;
          }

          /**
           * Required. The parent resource of the ConnectionProfile type. Must be in the format
           * `projects/locations`.
           */
          public Discover setParent(java.lang.String parent) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  PARENT_PATTERN.matcher(parent).matches(),
                  "Parameter parent must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+$");
            }
            this.parent = parent;
            return this;
          }

          @Override
          public Discover set(String parameterName, Object value) {
            return (Discover) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to get details about a connection profile.
         *
         * <p>Create a request for the method "connectionProfiles.get".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the Connection Profile resource to get.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model.ConnectionProfile> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");

          /**
           * Use this method to get details about a connection profile.
           *
           * <p>Create a request for the method "connectionProfiles.get".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the Connection Profile resource to get.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.ConnectionProfile.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");
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

          /** The name of the Connection Profile resource to get. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the Connection Profile resource to get. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the Connection Profile resource to get. */
          public Get setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");
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
         * Use this method to list connection profiles created in a project and location.
         *
         * <p>Create a request for the method "connectionProfiles.list".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent that owns the collection of Connection Profiles.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model.ListConnectionProfilesResponse> {

          private static final String REST_PATH = "v1alpha1/{+parent}/connectionProfiles";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to list connection profiles created in a project and location.
           *
           * <p>Create a request for the method "connectionProfiles.list".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of Connection Profiles.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.ListConnectionProfilesResponse
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

          /** Required. The parent that owns the collection of Connection Profiles. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of Connection Profiles. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of Connection Profiles. */
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

          /**
           * Maximum number of Connection Profiles to return. The service may return fewer than this
           * value. If unspecified, at most 50 Connection Profiles will be returned. The maximum
           * value is 1000; values above 1000 will be coerced to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Maximum number of Connection Profiles to return. The service may return fewer than this
           * value. If unspecified, at most 50 Connection Profiles will be returned. The maximum
           * value is 1000; values above 1000 will be coerced to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Maximum number of Connection Profiles to return. The service may return fewer than this
           * value. If unspecified, at most 50 Connection Profiles will be returned. The maximum
           * value is 1000; values above 1000 will be coerced to 1000.
           */
          public List setPageSize(java.lang.Integer pageSize) {
            this.pageSize = pageSize;
            return this;
          }

          /**
           * Page token received from a previous `ListConnectionProfiles` call. Provide this to
           * retrieve the subsequent page. When paginating, all other parameters provided to
           * `ListConnectionProfiles` must match the call that provided the page token.
           */
          @com.google.api.client.util.Key private java.lang.String pageToken;

          /**
           * Page token received from a previous `ListConnectionProfiles` call. Provide this to
           * retrieve the subsequent page. When paginating, all other parameters provided to
           * `ListConnectionProfiles` must match the call that provided the page token.
           */
          public java.lang.String getPageToken() {
            return pageToken;
          }

          /**
           * Page token received from a previous `ListConnectionProfiles` call. Provide this to
           * retrieve the subsequent page. When paginating, all other parameters provided to
           * `ListConnectionProfiles` must match the call that provided the page token.
           */
          public List setPageToken(java.lang.String pageToken) {
            this.pageToken = pageToken;
            return this;
          }

          /** Filter request. */
          @com.google.api.client.util.Key private java.lang.String filter;

          /** Filter request. */
          public java.lang.String getFilter() {
            return filter;
          }

          /** Filter request. */
          public List setFilter(java.lang.String filter) {
            this.filter = filter;
            return this;
          }

          /** Order by fields for the result. */
          @com.google.api.client.util.Key private java.lang.String orderBy;

          /** Order by fields for the result. */
          public java.lang.String getOrderBy() {
            return orderBy;
          }

          /** Order by fields for the result. */
          public List setOrderBy(java.lang.String orderBy) {
            this.orderBy = orderBy;
            return this;
          }

          @Override
          public List set(String parameterName, Object value) {
            return (List) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to update the parameters of a connection profile.
         *
         * <p>Create a request for the method "connectionProfiles.patch".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Patch#execute()} method to invoke the remote
         * operation.
         *
         * @param name Output only. The resource's name.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.ConnectionProfile}
         * @return the request
         */
        public Patch patch(
            java.lang.String name,
            com.google.api.services.datastream.v1alpha1.model.ConnectionProfile content)
            throws java.io.IOException {
          Patch result = new Patch(name, content);
          initialize(result);
          return result;
        }

        public class Patch
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");

          /**
           * Use this method to update the parameters of a connection profile.
           *
           * <p>Create a request for the method "connectionProfiles.patch".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Output only. The resource's name.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.ConnectionProfile}
           * @since 1.13
           */
          protected Patch(
              java.lang.String name,
              com.google.api.services.datastream.v1alpha1.model.ConnectionProfile content) {
            super(
                DataStream.this,
                "PATCH",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");
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

          /** Output only. The resource's name. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Output only. The resource's name. */
          public java.lang.String getName() {
            return name;
          }

          /** Output only. The resource's name. */
          public Patch setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /**
           * Field mask is used to specify the fields to be overwritten in the ConnectionProfile
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          @com.google.api.client.util.Key private String updateMask;

          /**
           * Field mask is used to specify the fields to be overwritten in the ConnectionProfile
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          public String getUpdateMask() {
            return updateMask;
          }

          /**
           * Field mask is used to specify the fields to be overwritten in the ConnectionProfile
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          public Patch setUpdateMask(String updateMask) {
            this.updateMask = updateMask;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Patch setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          @Override
          public Patch set(String parameterName, Object value) {
            return (Patch) super.set(parameterName, value);
          }
        }
      }
      /**
       * An accessor for creating requests from the Operations collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code DataStream datastream = new DataStream(...);}
       *   {@code DataStream.Operations.List request = datastream.operations().list(parameters ...)}
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
         * support this method, it returns `google.rpc.Code.UNIMPLEMENTED`. Clients can use
         * Operations.GetOperation or other methods to check whether the cancellation succeeded or
         * whether the operation completed despite cancellation. On successful cancellation, the
         * operation is not deleted; instead, it becomes an operation with an Operation.error value
         * with a google.rpc.Status.code of 1, corresponding to `Code.CANCELLED`.
         *
         * <p>Create a request for the method "operations.cancel".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Cancel#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the operation resource to be cancelled.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.CancelOperationRequest}
         * @return the request
         */
        public Cancel cancel(
            java.lang.String name,
            com.google.api.services.datastream.v1alpha1.model.CancelOperationRequest content)
            throws java.io.IOException {
          Cancel result = new Cancel(name, content);
          initialize(result);
          return result;
        }

        public class Cancel
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Empty> {

          private static final String REST_PATH = "v1alpha1/{+name}:cancel";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

          /**
           * Starts asynchronous cancellation on a long-running operation. The server makes a best
           * effort to cancel the operation, but success is not guaranteed. If the server doesn't
           * support this method, it returns `google.rpc.Code.UNIMPLEMENTED`. Clients can use
           * Operations.GetOperation or other methods to check whether the cancellation succeeded or
           * whether the operation completed despite cancellation. On successful cancellation, the
           * operation is not deleted; instead, it becomes an operation with an Operation.error
           * value with a google.rpc.Status.code of 1, corresponding to `Code.CANCELLED`.
           *
           * <p>Create a request for the method "operations.cancel".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Cancel#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Cancel#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource to be cancelled.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.CancelOperationRequest}
           * @since 1.13
           */
          protected Cancel(
              java.lang.String name,
              com.google.api.services.datastream.v1alpha1.model.CancelOperationRequest content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Empty.class);
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
         * doesn't support this method, it returns `google.rpc.Code.UNIMPLEMENTED`.
         *
         * <p>Create a request for the method "operations.delete".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
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
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Empty> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

          /**
           * Deletes a long-running operation. This method indicates that the client is no longer
           * interested in the operation result. It does not cancel the operation. If the server
           * doesn't support this method, it returns `google.rpc.Code.UNIMPLEMENTED`.
           *
           * <p>Create a request for the method "operations.delete".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
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
                DataStream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.Empty.class);
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
         * <p>This request holds the parameters needed by the datastream server. After setting any
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
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

          /**
           * Gets the latest state of a long-running operation. Clients can use this method to poll
           * the operation result at intervals as recommended by the API service.
           *
           * <p>Create a request for the method "operations.get".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
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
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
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
         * support this method, it returns `UNIMPLEMENTED`. NOTE: the `name` binding allows API
         * services to override the binding to use different resource name schemes, such as
         * `users/operations`. To override the binding, API services can add a binding such as
         * `"/v1/{name=users}/operations"` to their service configuration. For backwards
         * compatibility, the default name includes the operations collection id, however overriding
         * users must ensure the name binding is the parent resource, without the operations
         * collection id.
         *
         * <p>Create a request for the method "operations.list".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
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
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model.ListOperationsResponse> {

          private static final String REST_PATH = "v1alpha1/{+name}/operations";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Lists operations that match the specified filter in the request. If the server doesn't
           * support this method, it returns `UNIMPLEMENTED`. NOTE: the `name` binding allows API
           * services to override the binding to use different resource name schemes, such as
           * `users/operations`. To override the binding, API services can add a binding such as
           * `"/v1/{name=users}/operations"` to their service configuration. For backwards
           * compatibility, the default name includes the operations collection id, however
           * overriding users must ensure the name binding is the parent resource, without the
           * operations collection id.
           *
           * <p>Create a request for the method "operations.list".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
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
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.ListOperationsResponse.class);
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
      /**
       * An accessor for creating requests from the PrivateConnections collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code DataStream datastream = new DataStream(...);}
       *   {@code DataStream.PrivateConnections.List request = datastream.privateConnections().list(parameters ...)}
       * </pre>
       *
       * @return the resource collection
       */
      public PrivateConnections privateConnections() {
        return new PrivateConnections();
      }

      /** The "privateConnections" collection of methods. */
      public class PrivateConnections {

        /**
         * Use this method to create a private connection in a project and location.
         *
         * <p>Create a request for the method "privateConnections.create".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Create#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent that owns the collection of PrivateConnections.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.PrivateConnection}
         * @return the request
         */
        public Create create(
            java.lang.String parent,
            com.google.api.services.datastream.v1alpha1.model.PrivateConnection content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+parent}/privateConnections";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to create a private connection in a project and location.
           *
           * <p>Create a request for the method "privateConnections.create".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of PrivateConnections.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.PrivateConnection}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent,
              com.google.api.services.datastream.v1alpha1.model.PrivateConnection content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
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

          /** Required. The parent that owns the collection of PrivateConnections. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of PrivateConnections. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of PrivateConnections. */
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

          /** Required. The Private Connection identifier. */
          @com.google.api.client.util.Key private java.lang.String privateConnectionId;

          /** Required. The Private Connection identifier. */
          public java.lang.String getPrivateConnectionId() {
            return privateConnectionId;
          }

          /** Required. The Private Connection identifier. */
          public Create setPrivateConnectionId(java.lang.String privateConnectionId) {
            this.privateConnectionId = privateConnectionId;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Create setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          @Override
          public Create set(String parameterName, Object value) {
            return (Create) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to delete a private connection .
         *
         * <p>Create a request for the method "privateConnections.delete".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the Private Connection resource to delete.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

          /**
           * Use this method to delete a private connection .
           *
           * <p>Create a request for the method "privateConnections.delete".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the Private Connection resource to delete.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                DataStream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
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

          /** Required. The name of the Private Connection resource to delete. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the Private Connection resource to delete. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the Private Connection resource to delete. */
          public Delete setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Delete setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          /**
           * Optional. If set to true, any child routes that belong to this PrivateConnection will
           * also be deleted.
           */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /**
           * Optional. If set to true, any child routes that belong to this PrivateConnection will
           * also be deleted.
           */
          public java.lang.Boolean getForce() {
            return force;
          }

          /**
           * Optional. If set to true, any child routes that belong to this PrivateConnection will
           * also be deleted.
           */
          public Delete setForce(java.lang.Boolean force) {
            this.force = force;
            return this;
          }

          @Override
          public Delete set(String parameterName, Object value) {
            return (Delete) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to get details about a private connection.
         *
         * <p>Create a request for the method "privateConnections.get".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the Private Connection resource to get.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model.PrivateConnection> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

          /**
           * Use this method to get details about a private connection.
           *
           * <p>Create a request for the method "privateConnections.get".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the Private Connection resource to get.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.PrivateConnection.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
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

          /** Required. The name of the Private Connection resource to get. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the Private Connection resource to get. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the Private Connection resource to get. */
          public Get setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
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
         * Use this method to list private connections created in a project and location.
         *
         * <p>Create a request for the method "privateConnections.list".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent that owns the collection of Private Connections.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model.ListPrivateConnectionsResponse> {

          private static final String REST_PATH = "v1alpha1/{+parent}/privateConnections";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to list private connections created in a project and location.
           *
           * <p>Create a request for the method "privateConnections.list".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of Private Connections.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.ListPrivateConnectionsResponse
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

          /** Required. The parent that owns the collection of Private Connections. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of Private Connections. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of Private Connections. */
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

          /**
           * Maximum number of Private Connections to return. The service may return fewer than this
           * value. If unspecified, at most 50 Private Connections will be returned. The maximum
           * value is 1000; values above 1000 will be coerced to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Maximum number of Private Connections to return. The service may return fewer than this
           * value. If unspecified, at most 50 Private Connections will be returned. The maximum
           * value is 1000; values above 1000 will be coerced to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Maximum number of Private Connections to return. The service may return fewer than this
           * value. If unspecified, at most 50 Private Connections will be returned. The maximum
           * value is 1000; values above 1000 will be coerced to 1000.
           */
          public List setPageSize(java.lang.Integer pageSize) {
            this.pageSize = pageSize;
            return this;
          }

          /**
           * Page token received from a previous `ListPrivateConnections` call. Provide this to
           * retrieve the subsequent page. When paginating, all other parameters provided to
           * `ListPrivateConnections` must match the call that provided the page token.
           */
          @com.google.api.client.util.Key private java.lang.String pageToken;

          /**
           * Page token received from a previous `ListPrivateConnections` call. Provide this to
           * retrieve the subsequent page. When paginating, all other parameters provided to
           * `ListPrivateConnections` must match the call that provided the page token.
           */
          public java.lang.String getPageToken() {
            return pageToken;
          }

          /**
           * Page token received from a previous `ListPrivateConnections` call. Provide this to
           * retrieve the subsequent page. When paginating, all other parameters provided to
           * `ListPrivateConnections` must match the call that provided the page token.
           */
          public List setPageToken(java.lang.String pageToken) {
            this.pageToken = pageToken;
            return this;
          }

          /** Filter request. */
          @com.google.api.client.util.Key private java.lang.String filter;

          /** Filter request. */
          public java.lang.String getFilter() {
            return filter;
          }

          /** Filter request. */
          public List setFilter(java.lang.String filter) {
            this.filter = filter;
            return this;
          }

          /** Order by fields for the result. */
          @com.google.api.client.util.Key private java.lang.String orderBy;

          /** Order by fields for the result. */
          public java.lang.String getOrderBy() {
            return orderBy;
          }

          /** Order by fields for the result. */
          public List setOrderBy(java.lang.String orderBy) {
            this.orderBy = orderBy;
            return this;
          }

          @Override
          public List set(String parameterName, Object value) {
            return (List) super.set(parameterName, value);
          }
        }

        /**
         * An accessor for creating requests from the Routes collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code DataStream datastream = new DataStream(...);}
         *   {@code DataStream.Routes.List request = datastream.routes().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Routes routes() {
          return new Routes();
        }

        /** The "routes" collection of methods. */
        public class Routes {

          /**
           * Use this method to create a Route for a private connection, in a project and location.
           *
           * <p>Create a request for the method "routes.create".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The parent that owns the collection of Routes.
           * @param content the {@link com.google.api.services.datastream.v1alpha1.model.Route}
           * @return the request
           */
          public Create create(
              java.lang.String parent,
              com.google.api.services.datastream.v1alpha1.model.Route content)
              throws java.io.IOException {
            Create result = new Create(parent, content);
            initialize(result);
            return result;
          }

          public class Create
              extends DataStreamRequest<
                  com.google.api.services.datastream.v1alpha1.model.Operation> {

            private static final String REST_PATH = "v1alpha1/{+parent}/routes";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

            /**
             * Use this method to create a Route for a private connection, in a project and
             * location.
             *
             * <p>Create a request for the method "routes.create".
             *
             * <p>This request holds the parameters needed by the the datastream server. After
             * setting any optional parameters, call the {@link Create#execute()} method to invoke
             * the remote operation.
             *
             * <p>{@link
             * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The parent that owns the collection of Routes.
             * @param content the {@link com.google.api.services.datastream.v1alpha1.model.Route}
             * @since 1.13
             */
            protected Create(
                java.lang.String parent,
                com.google.api.services.datastream.v1alpha1.model.Route content) {
              super(
                  DataStream.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.datastream.v1alpha1.model.Operation.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
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

            /** Required. The parent that owns the collection of Routes. */
            @com.google.api.client.util.Key private java.lang.String parent;

            /** Required. The parent that owns the collection of Routes. */
            public java.lang.String getParent() {
              return parent;
            }

            /** Required. The parent that owns the collection of Routes. */
            public Create setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /** Required. The Route identifier. */
            @com.google.api.client.util.Key private java.lang.String routeId;

            /** Required. The Route identifier. */
            public java.lang.String getRouteId() {
              return routeId;
            }

            /** Required. The Route identifier. */
            public Create setRouteId(java.lang.String routeId) {
              this.routeId = routeId;
              return this;
            }

            /**
             * Optional. A request ID to identify requests. Specify a unique request ID so that if
             * you must retry your request, the server will know to ignore the request if it has
             * already been completed. The server will guarantee that for at least 60 minutes since
             * the first request. For example, consider a situation where you make an initial
             * request and t he request times out. If you make the request again with the same
             * request ID, the server can check if original operation with the same request ID was
             * received, and if so, will ignore the second request. This prevents clients from
             * accidentally creating duplicate commitments. The request ID must be a valid UUID with
             * the exception that zero UUID is not supported (00000000-0000-0000-0000-000000000000).
             */
            @com.google.api.client.util.Key private java.lang.String requestId;

            /**
             * Optional. A request ID to identify requests. Specify a unique request ID so that if
             * you must retry your request, the server will know to ignore the request if it has
             * already been completed. The server will guarantee that for at least 60 minutes since
             * the first request. For example, consider a situation where you make an initial
             * request and t he request times out. If you make the request again with the same
             * request ID, the server can check if original operation with the same request ID was
             * received, and if so, will ignore the second request. This prevents clients from
             * accidentally creating duplicate commitments. The request ID must be a valid UUID with
             * the exception that zero UUID is not supported (00000000-0000-0000-0000-000000000000).
             */
            public java.lang.String getRequestId() {
              return requestId;
            }

            /**
             * Optional. A request ID to identify requests. Specify a unique request ID so that if
             * you must retry your request, the server will know to ignore the request if it has
             * already been completed. The server will guarantee that for at least 60 minutes since
             * the first request. For example, consider a situation where you make an initial
             * request and t he request times out. If you make the request again with the same
             * request ID, the server can check if original operation with the same request ID was
             * received, and if so, will ignore the second request. This prevents clients from
             * accidentally creating duplicate commitments. The request ID must be a valid UUID with
             * the exception that zero UUID is not supported (00000000-0000-0000-0000-000000000000).
             */
            public Create setRequestId(java.lang.String requestId) {
              this.requestId = requestId;
              return this;
            }

            @Override
            public Create set(String parameterName, Object value) {
              return (Create) super.set(parameterName, value);
            }
          }
          /**
           * Use this method to delete a route.
           *
           * <p>Create a request for the method "routes.delete".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The name of the Route resource to delete.
           * @return the request
           */
          public Delete delete(java.lang.String name) throws java.io.IOException {
            Delete result = new Delete(name);
            initialize(result);
            return result;
          }

          public class Delete
              extends DataStreamRequest<
                  com.google.api.services.datastream.v1alpha1.model.Operation> {

            private static final String REST_PATH = "v1alpha1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");

            /**
             * Use this method to delete a route.
             *
             * <p>Create a request for the method "routes.delete".
             *
             * <p>This request holds the parameters needed by the the datastream server. After
             * setting any optional parameters, call the {@link Delete#execute()} method to invoke
             * the remote operation.
             *
             * <p>{@link
             * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The name of the Route resource to delete.
             * @since 1.13
             */
            protected Delete(java.lang.String name) {
              super(
                  DataStream.this,
                  "DELETE",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1alpha1.model.Operation.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");
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

            /** Required. The name of the Route resource to delete. */
            @com.google.api.client.util.Key private java.lang.String name;

            /** Required. The name of the Route resource to delete. */
            public java.lang.String getName() {
              return name;
            }

            /** Required. The name of the Route resource to delete. */
            public Delete setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");
              }
              this.name = name;
              return this;
            }

            /**
             * Optional. A request ID to identify requests. Specify a unique request ID so that if
             * you must retry your request, the server will know to ignore the request if it has
             * already been completed. The server will guarantee that for at least 60 minutes after
             * the first request. For example, consider a situation where you make an initial
             * request and t he request times out. If you make the request again with the same
             * request ID, the server can check if original operation with the same request ID was
             * received, and if so, will ignore the second request. This prevents clients from
             * accidentally creating duplicate commitments. The request ID must be a valid UUID with
             * the exception that zero UUID is not supported (00000000-0000-0000-0000-000000000000).
             */
            @com.google.api.client.util.Key private java.lang.String requestId;

            /**
             * Optional. A request ID to identify requests. Specify a unique request ID so that if
             * you must retry your request, the server will know to ignore the request if it has
             * already been completed. The server will guarantee that for at least 60 minutes after
             * the first request. For example, consider a situation where you make an initial
             * request and t he request times out. If you make the request again with the same
             * request ID, the server can check if original operation with the same request ID was
             * received, and if so, will ignore the second request. This prevents clients from
             * accidentally creating duplicate commitments. The request ID must be a valid UUID with
             * the exception that zero UUID is not supported (00000000-0000-0000-0000-000000000000).
             */
            public java.lang.String getRequestId() {
              return requestId;
            }

            /**
             * Optional. A request ID to identify requests. Specify a unique request ID so that if
             * you must retry your request, the server will know to ignore the request if it has
             * already been completed. The server will guarantee that for at least 60 minutes after
             * the first request. For example, consider a situation where you make an initial
             * request and t he request times out. If you make the request again with the same
             * request ID, the server can check if original operation with the same request ID was
             * received, and if so, will ignore the second request. This prevents clients from
             * accidentally creating duplicate commitments. The request ID must be a valid UUID with
             * the exception that zero UUID is not supported (00000000-0000-0000-0000-000000000000).
             */
            public Delete setRequestId(java.lang.String requestId) {
              this.requestId = requestId;
              return this;
            }

            @Override
            public Delete set(String parameterName, Object value) {
              return (Delete) super.set(parameterName, value);
            }
          }
          /**
           * Use this method to get details about a route.
           *
           * <p>Create a request for the method "routes.get".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The name of the Route resource to get.
           * @return the request
           */
          public Get get(java.lang.String name) throws java.io.IOException {
            Get result = new Get(name);
            initialize(result);
            return result;
          }

          public class Get
              extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Route> {

            private static final String REST_PATH = "v1alpha1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");

            /**
             * Use this method to get details about a route.
             *
             * <p>Create a request for the method "routes.get".
             *
             * <p>This request holds the parameters needed by the the datastream server. After
             * setting any optional parameters, call the {@link Get#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The name of the Route resource to get.
             * @since 1.13
             */
            protected Get(java.lang.String name) {
              super(
                  DataStream.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1alpha1.model.Route.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");
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

            /** Required. The name of the Route resource to get. */
            @com.google.api.client.util.Key private java.lang.String name;

            /** Required. The name of the Route resource to get. */
            public java.lang.String getName() {
              return name;
            }

            /** Required. The name of the Route resource to get. */
            public Get setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");
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
           * Use this method to list routes created for a private connection in a project and
           * location.
           *
           * <p>Create a request for the method "routes.list".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The parent that owns the collection of Routess.
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends DataStreamRequest<
                  com.google.api.services.datastream.v1alpha1.model.ListRoutesResponse> {

            private static final String REST_PATH = "v1alpha1/{+parent}/routes";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

            /**
             * Use this method to list routes created for a private connection in a project and
             * location.
             *
             * <p>Create a request for the method "routes.list".
             *
             * <p>This request holds the parameters needed by the the datastream server. After
             * setting any optional parameters, call the {@link List#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The parent that owns the collection of Routess.
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  DataStream.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1alpha1.model.ListRoutesResponse.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
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

            /** Required. The parent that owns the collection of Routess. */
            @com.google.api.client.util.Key private java.lang.String parent;

            /** Required. The parent that owns the collection of Routess. */
            public java.lang.String getParent() {
              return parent;
            }

            /** Required. The parent that owns the collection of Routess. */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Maximum number of Routes to return. The service may return fewer than this value. If
             * unspecified, at most 50 Routes will be returned. The maximum value is 1000; values
             * above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Maximum number of Routes to return. The service may return fewer than this value. If
             * unspecified, at most 50 Routes will be returned. The maximum value is 1000; values
             * above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Maximum number of Routes to return. The service may return fewer than this value. If
             * unspecified, at most 50 Routes will be returned. The maximum value is 1000; values
             * above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Page token received from a previous `ListRoutes` call. Provide this to retrieve the
             * subsequent page. When paginating, all other parameters provided to `ListRoutes` must
             * match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Page token received from a previous `ListRoutes` call. Provide this to retrieve the
             * subsequent page. When paginating, all other parameters provided to `ListRoutes` must
             * match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Page token received from a previous `ListRoutes` call. Provide this to retrieve the
             * subsequent page. When paginating, all other parameters provided to `ListRoutes` must
             * match the call that provided the page token.
             */
            public List setPageToken(java.lang.String pageToken) {
              this.pageToken = pageToken;
              return this;
            }

            /** Filter request. */
            @com.google.api.client.util.Key private java.lang.String filter;

            /** Filter request. */
            public java.lang.String getFilter() {
              return filter;
            }

            /** Filter request. */
            public List setFilter(java.lang.String filter) {
              this.filter = filter;
              return this;
            }

            /** Order by fields for the result. */
            @com.google.api.client.util.Key private java.lang.String orderBy;

            /** Order by fields for the result. */
            public java.lang.String getOrderBy() {
              return orderBy;
            }

            /** Order by fields for the result. */
            public List setOrderBy(java.lang.String orderBy) {
              this.orderBy = orderBy;
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
       * An accessor for creating requests from the Streams collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code DataStream datastream = new DataStream(...);}
       *   {@code DataStream.Streams.List request = datastream.streams().list(parameters ...)}
       * </pre>
       *
       * @return the resource collection
       */
      public Streams streams() {
        return new Streams();
      }

      /** The "streams" collection of methods. */
      public class Streams {

        /**
         * Use this method to create a stream.
         *
         * <p>Create a request for the method "streams.create".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Create#execute()} method to invoke the remote
         * operation.
         *
         * @param parent The parent that owns the collection of Streams.
         * @param content the {@link com.google.api.services.datastream.v1alpha1.model.Stream}
         * @return the request
         */
        public Create create(
            java.lang.String parent,
            com.google.api.services.datastream.v1alpha1.model.Stream content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+parent}/streams";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to create a stream.
           *
           * <p>Create a request for the method "streams.create".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent The parent that owns the collection of Streams.
           * @param content the {@link com.google.api.services.datastream.v1alpha1.model.Stream}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent,
              com.google.api.services.datastream.v1alpha1.model.Stream content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
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

          /** The parent that owns the collection of Streams. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** The parent that owns the collection of Streams. */
          public java.lang.String getParent() {
            return parent;
          }

          /** The parent that owns the collection of Streams. */
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

          /** The Stream identifier. */
          @com.google.api.client.util.Key private java.lang.String streamId;

          /** The Stream identifier. */
          public java.lang.String getStreamId() {
            return streamId;
          }

          /** The Stream identifier. */
          public Create setStreamId(java.lang.String streamId) {
            this.streamId = streamId;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Create setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          /**
           * Optional. Only validate the creation, does not create any resources, defaults to false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the creation, does not create any resources, defaults to false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the creation, does not create any resources, defaults to false.
           */
          public Create setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          /** Optional. Execute the create without validating it. */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /** Optional. Execute the create without validating it. */
          public java.lang.Boolean getForce() {
            return force;
          }

          /** Optional. Execute the create without validating it. */
          public Create setForce(java.lang.Boolean force) {
            this.force = force;
            return this;
          }

          @Override
          public Create set(String parameterName, Object value) {
            return (Create) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to delete a stream.
         *
         * <p>Create a request for the method "streams.delete".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the Stream resource to delete.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to delete a stream.
           *
           * <p>Create a request for the method "streams.delete".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the Stream resource to delete.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                DataStream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
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

          /** Required. The name of the Stream resource to delete. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the Stream resource to delete. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the Stream resource to delete. */
          public Delete setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes after the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Delete setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          @Override
          public Delete set(String parameterName, Object value) {
            return (Delete) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to fetch any errors associated with a stream.
         *
         * <p>Create a request for the method "streams.fetchErrors".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link FetchErrors#execute()} method to invoke the remote
         * operation.
         *
         * @param stream Name of the Stream resource for which to fetch any errors.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.FetchErrorsRequest}
         * @return the request
         */
        public FetchErrors fetchErrors(
            java.lang.String stream,
            com.google.api.services.datastream.v1alpha1.model.FetchErrorsRequest content)
            throws java.io.IOException {
          FetchErrors result = new FetchErrors(stream, content);
          initialize(result);
          return result;
        }

        public class FetchErrors
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+stream}:fetchErrors";

          private final java.util.regex.Pattern STREAM_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to fetch any errors associated with a stream.
           *
           * <p>Create a request for the method "streams.fetchErrors".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link FetchErrors#execute()} method to invoke the
           * remote operation.
           *
           * <p>{@link
           * FetchErrors#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param stream Name of the Stream resource for which to fetch any errors.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.FetchErrorsRequest}
           * @since 1.13
           */
          protected FetchErrors(
              java.lang.String stream,
              com.google.api.services.datastream.v1alpha1.model.FetchErrorsRequest content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.stream =
                com.google.api.client.util.Preconditions.checkNotNull(
                    stream, "Required parameter stream must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  STREAM_PATTERN.matcher(stream).matches(),
                  "Parameter stream must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
          }

          @Override
          public FetchErrors set$Xgafv(java.lang.String $Xgafv) {
            return (FetchErrors) super.set$Xgafv($Xgafv);
          }

          @Override
          public FetchErrors setAccessToken(java.lang.String accessToken) {
            return (FetchErrors) super.setAccessToken(accessToken);
          }

          @Override
          public FetchErrors setAlt(java.lang.String alt) {
            return (FetchErrors) super.setAlt(alt);
          }

          @Override
          public FetchErrors setCallback(java.lang.String callback) {
            return (FetchErrors) super.setCallback(callback);
          }

          @Override
          public FetchErrors setFields(java.lang.String fields) {
            return (FetchErrors) super.setFields(fields);
          }

          @Override
          public FetchErrors setKey(java.lang.String key) {
            return (FetchErrors) super.setKey(key);
          }

          @Override
          public FetchErrors setOauthToken(java.lang.String oauthToken) {
            return (FetchErrors) super.setOauthToken(oauthToken);
          }

          @Override
          public FetchErrors setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (FetchErrors) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public FetchErrors setQuotaUser(java.lang.String quotaUser) {
            return (FetchErrors) super.setQuotaUser(quotaUser);
          }

          @Override
          public FetchErrors setUploadType(java.lang.String uploadType) {
            return (FetchErrors) super.setUploadType(uploadType);
          }

          @Override
          public FetchErrors setUploadProtocol(java.lang.String uploadProtocol) {
            return (FetchErrors) super.setUploadProtocol(uploadProtocol);
          }

          /** Name of the Stream resource for which to fetch any errors. */
          @com.google.api.client.util.Key private java.lang.String stream;

          /** Name of the Stream resource for which to fetch any errors. */
          public java.lang.String getStream() {
            return stream;
          }

          /** Name of the Stream resource for which to fetch any errors. */
          public FetchErrors setStream(java.lang.String stream) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  STREAM_PATTERN.matcher(stream).matches(),
                  "Parameter stream must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
            this.stream = stream;
            return this;
          }

          @Override
          public FetchErrors set(String parameterName, Object value) {
            return (FetchErrors) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to get details about a stream.
         *
         * <p>Create a request for the method "streams.get".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name The name of the Stream resource to get.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Stream> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to get details about a stream.
           *
           * <p>Create a request for the method "streams.get".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the Stream resource to get.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.Stream.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
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

          /** The name of the Stream resource to get. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** The name of the Stream resource to get. */
          public java.lang.String getName() {
            return name;
          }

          /** The name of the Stream resource to get. */
          public Get setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
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
         * Use this method to list streams in a project and location.
         *
         * <p>Create a request for the method "streams.list".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent that owns the collection of Streams.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends DataStreamRequest<
                com.google.api.services.datastream.v1alpha1.model.ListStreamsResponse> {

          private static final String REST_PATH = "v1alpha1/{+parent}/streams";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to list streams in a project and location.
           *
           * <p>Create a request for the method "streams.list".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of Streams.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                DataStream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1alpha1.model.ListStreamsResponse.class);
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

          /** Required. The parent that owns the collection of Streams. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of Streams. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of Streams. */
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

          /**
           * Maximum number of Streams to return. The service may return fewer than this value. If
           * unspecified, at most 50 Streams will be returned. The maximum value is 1000; values
           * above 1000 will be coerced to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Maximum number of Streams to return. The service may return fewer than this value. If
           * unspecified, at most 50 Streams will be returned. The maximum value is 1000; values
           * above 1000 will be coerced to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Maximum number of Streams to return. The service may return fewer than this value. If
           * unspecified, at most 50 Streams will be returned. The maximum value is 1000; values
           * above 1000 will be coerced to 1000.
           */
          public List setPageSize(java.lang.Integer pageSize) {
            this.pageSize = pageSize;
            return this;
          }

          /**
           * Page token received from a previous `ListStreams` call. Provide this to retrieve the
           * subsequent page. When paginating, all other parameters provided to `ListStreams` must
           * match the call that provided the page token.
           */
          @com.google.api.client.util.Key private java.lang.String pageToken;

          /**
           * Page token received from a previous `ListStreams` call. Provide this to retrieve the
           * subsequent page. When paginating, all other parameters provided to `ListStreams` must
           * match the call that provided the page token.
           */
          public java.lang.String getPageToken() {
            return pageToken;
          }

          /**
           * Page token received from a previous `ListStreams` call. Provide this to retrieve the
           * subsequent page. When paginating, all other parameters provided to `ListStreams` must
           * match the call that provided the page token.
           */
          public List setPageToken(java.lang.String pageToken) {
            this.pageToken = pageToken;
            return this;
          }

          /** Filter request. */
          @com.google.api.client.util.Key private java.lang.String filter;

          /** Filter request. */
          public java.lang.String getFilter() {
            return filter;
          }

          /** Filter request. */
          public List setFilter(java.lang.String filter) {
            this.filter = filter;
            return this;
          }

          /** Order by fields for the result. */
          @com.google.api.client.util.Key private java.lang.String orderBy;

          /** Order by fields for the result. */
          public java.lang.String getOrderBy() {
            return orderBy;
          }

          /** Order by fields for the result. */
          public List setOrderBy(java.lang.String orderBy) {
            this.orderBy = orderBy;
            return this;
          }

          @Override
          public List set(String parameterName, Object value) {
            return (List) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to update the configuration of a stream.
         *
         * <p>Create a request for the method "streams.patch".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Patch#execute()} method to invoke the remote
         * operation.
         *
         * @param name Output only. The resource's name.
         * @param content the {@link com.google.api.services.datastream.v1alpha1.model.Stream}
         * @return the request
         */
        public Patch patch(
            java.lang.String name, com.google.api.services.datastream.v1alpha1.model.Stream content)
            throws java.io.IOException {
          Patch result = new Patch(name, content);
          initialize(result);
          return result;
        }

        public class Patch
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to update the configuration of a stream.
           *
           * <p>Create a request for the method "streams.patch".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Output only. The resource's name.
           * @param content the {@link com.google.api.services.datastream.v1alpha1.model.Stream}
           * @since 1.13
           */
          protected Patch(
              java.lang.String name,
              com.google.api.services.datastream.v1alpha1.model.Stream content) {
            super(
                DataStream.this,
                "PATCH",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
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

          /** Output only. The resource's name. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Output only. The resource's name. */
          public java.lang.String getName() {
            return name;
          }

          /** Output only. The resource's name. */
          public Patch setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
            this.name = name;
            return this;
          }

          /**
           * Required. Field mask is used to specify the fields to be overwritten in the Stream
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          @com.google.api.client.util.Key private String updateMask;

          /**
           * Required. Field mask is used to specify the fields to be overwritten in the Stream
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          public String getUpdateMask() {
            return updateMask;
          }

          /**
           * Required. Field mask is used to specify the fields to be overwritten in the Stream
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          public Patch setUpdateMask(String updateMask) {
            this.updateMask = updateMask;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          @com.google.api.client.util.Key private java.lang.String requestId;

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public java.lang.String getRequestId() {
            return requestId;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and t he
           * request times out. If you make the request again with the same request ID, the server
           * can check if original operation with the same request ID was received, and if so, will
           * ignore the second request. This prevents clients from accidentally creating duplicate
           * commitments. The request ID must be a valid UUID with the exception that zero UUID is
           * not supported (00000000-0000-0000-0000-000000000000).
           */
          public Patch setRequestId(java.lang.String requestId) {
            this.requestId = requestId;
            return this;
          }

          /**
           * Optional. Only validate the update, does not create any resources, defaults to false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the update, does not create any resources, defaults to false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the update, does not create any resources, defaults to false.
           */
          public Patch setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          /** Optional. Execute the update without validating it. */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /** Optional. Execute the update without validating it. */
          public java.lang.Boolean getForce() {
            return force;
          }

          /** Optional. Execute the update without validating it. */
          public Patch setForce(java.lang.Boolean force) {
            this.force = force;
            return this;
          }

          @Override
          public Patch set(String parameterName, Object value) {
            return (Patch) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to pause a running stream.
         *
         * <p>Create a request for the method "streams.pause".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Pause#execute()} method to invoke the remote
         * operation.
         *
         * @param name Name of the Stream resource to pause.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.PauseStreamRequest}
         * @return the request
         */
        public Pause pause(
            java.lang.String name,
            com.google.api.services.datastream.v1alpha1.model.PauseStreamRequest content)
            throws java.io.IOException {
          Pause result = new Pause(name, content);
          initialize(result);
          return result;
        }

        public class Pause
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}:pause";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to pause a running stream.
           *
           * <p>Create a request for the method "streams.pause".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Pause#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Pause#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Name of the Stream resource to pause.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.PauseStreamRequest}
           * @since 1.13
           */
          protected Pause(
              java.lang.String name,
              com.google.api.services.datastream.v1alpha1.model.PauseStreamRequest content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
          }

          @Override
          public Pause set$Xgafv(java.lang.String $Xgafv) {
            return (Pause) super.set$Xgafv($Xgafv);
          }

          @Override
          public Pause setAccessToken(java.lang.String accessToken) {
            return (Pause) super.setAccessToken(accessToken);
          }

          @Override
          public Pause setAlt(java.lang.String alt) {
            return (Pause) super.setAlt(alt);
          }

          @Override
          public Pause setCallback(java.lang.String callback) {
            return (Pause) super.setCallback(callback);
          }

          @Override
          public Pause setFields(java.lang.String fields) {
            return (Pause) super.setFields(fields);
          }

          @Override
          public Pause setKey(java.lang.String key) {
            return (Pause) super.setKey(key);
          }

          @Override
          public Pause setOauthToken(java.lang.String oauthToken) {
            return (Pause) super.setOauthToken(oauthToken);
          }

          @Override
          public Pause setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Pause) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Pause setQuotaUser(java.lang.String quotaUser) {
            return (Pause) super.setQuotaUser(quotaUser);
          }

          @Override
          public Pause setUploadType(java.lang.String uploadType) {
            return (Pause) super.setUploadType(uploadType);
          }

          @Override
          public Pause setUploadProtocol(java.lang.String uploadProtocol) {
            return (Pause) super.setUploadProtocol(uploadProtocol);
          }

          /** Name of the Stream resource to pause. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Name of the Stream resource to pause. */
          public java.lang.String getName() {
            return name;
          }

          /** Name of the Stream resource to pause. */
          public Pause setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Pause set(String parameterName, Object value) {
            return (Pause) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to resume a paused stream.
         *
         * <p>Create a request for the method "streams.resume".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Resume#execute()} method to invoke the remote
         * operation.
         *
         * @param name Name of the Stream resource to resume.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.ResumeStreamRequest}
         * @return the request
         */
        public Resume resume(
            java.lang.String name,
            com.google.api.services.datastream.v1alpha1.model.ResumeStreamRequest content)
            throws java.io.IOException {
          Resume result = new Resume(name, content);
          initialize(result);
          return result;
        }

        public class Resume
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}:resume";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to resume a paused stream.
           *
           * <p>Create a request for the method "streams.resume".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Resume#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Resume#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Name of the Stream resource to resume.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.ResumeStreamRequest}
           * @since 1.13
           */
          protected Resume(
              java.lang.String name,
              com.google.api.services.datastream.v1alpha1.model.ResumeStreamRequest content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
          }

          @Override
          public Resume set$Xgafv(java.lang.String $Xgafv) {
            return (Resume) super.set$Xgafv($Xgafv);
          }

          @Override
          public Resume setAccessToken(java.lang.String accessToken) {
            return (Resume) super.setAccessToken(accessToken);
          }

          @Override
          public Resume setAlt(java.lang.String alt) {
            return (Resume) super.setAlt(alt);
          }

          @Override
          public Resume setCallback(java.lang.String callback) {
            return (Resume) super.setCallback(callback);
          }

          @Override
          public Resume setFields(java.lang.String fields) {
            return (Resume) super.setFields(fields);
          }

          @Override
          public Resume setKey(java.lang.String key) {
            return (Resume) super.setKey(key);
          }

          @Override
          public Resume setOauthToken(java.lang.String oauthToken) {
            return (Resume) super.setOauthToken(oauthToken);
          }

          @Override
          public Resume setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Resume) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Resume setQuotaUser(java.lang.String quotaUser) {
            return (Resume) super.setQuotaUser(quotaUser);
          }

          @Override
          public Resume setUploadType(java.lang.String uploadType) {
            return (Resume) super.setUploadType(uploadType);
          }

          @Override
          public Resume setUploadProtocol(java.lang.String uploadProtocol) {
            return (Resume) super.setUploadProtocol(uploadProtocol);
          }

          /** Name of the Stream resource to resume. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Name of the Stream resource to resume. */
          public java.lang.String getName() {
            return name;
          }

          /** Name of the Stream resource to resume. */
          public Resume setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Resume set(String parameterName, Object value) {
            return (Resume) super.set(parameterName, value);
          }
        }
        /**
         * Use this method to start a stream.
         *
         * <p>Create a request for the method "streams.start".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Start#execute()} method to invoke the remote
         * operation.
         *
         * @param name Name of the Stream resource to start.
         * @param content the {@link
         *     com.google.api.services.datastream.v1alpha1.model.StartStreamRequest}
         * @return the request
         */
        public Start start(
            java.lang.String name,
            com.google.api.services.datastream.v1alpha1.model.StartStreamRequest content)
            throws java.io.IOException {
          Start result = new Start(name, content);
          initialize(result);
          return result;
        }

        public class Start
            extends DataStreamRequest<com.google.api.services.datastream.v1alpha1.model.Operation> {

          private static final String REST_PATH = "v1alpha1/{+name}:start";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to start a stream.
           *
           * <p>Create a request for the method "streams.start".
           *
           * <p>This request holds the parameters needed by the the datastream server. After setting
           * any optional parameters, call the {@link Start#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Start#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Name of the Stream resource to start.
           * @param content the {@link
           *     com.google.api.services.datastream.v1alpha1.model.StartStreamRequest}
           * @since 1.13
           */
          protected Start(
              java.lang.String name,
              com.google.api.services.datastream.v1alpha1.model.StartStreamRequest content) {
            super(
                DataStream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1alpha1.model.Operation.class);
            this.name =
                com.google.api.client.util.Preconditions.checkNotNull(
                    name, "Required parameter name must be specified.");
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
          }

          @Override
          public Start set$Xgafv(java.lang.String $Xgafv) {
            return (Start) super.set$Xgafv($Xgafv);
          }

          @Override
          public Start setAccessToken(java.lang.String accessToken) {
            return (Start) super.setAccessToken(accessToken);
          }

          @Override
          public Start setAlt(java.lang.String alt) {
            return (Start) super.setAlt(alt);
          }

          @Override
          public Start setCallback(java.lang.String callback) {
            return (Start) super.setCallback(callback);
          }

          @Override
          public Start setFields(java.lang.String fields) {
            return (Start) super.setFields(fields);
          }

          @Override
          public Start setKey(java.lang.String key) {
            return (Start) super.setKey(key);
          }

          @Override
          public Start setOauthToken(java.lang.String oauthToken) {
            return (Start) super.setOauthToken(oauthToken);
          }

          @Override
          public Start setPrettyPrint(java.lang.Boolean prettyPrint) {
            return (Start) super.setPrettyPrint(prettyPrint);
          }

          @Override
          public Start setQuotaUser(java.lang.String quotaUser) {
            return (Start) super.setQuotaUser(quotaUser);
          }

          @Override
          public Start setUploadType(java.lang.String uploadType) {
            return (Start) super.setUploadType(uploadType);
          }

          @Override
          public Start setUploadProtocol(java.lang.String uploadProtocol) {
            return (Start) super.setUploadProtocol(uploadProtocol);
          }

          /** Name of the Stream resource to start. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Name of the Stream resource to start. */
          public java.lang.String getName() {
            return name;
          }

          /** Name of the Stream resource to start. */
          public Start setName(java.lang.String name) {
            if (!getSuppressPatternChecks()) {
              com.google.api.client.util.Preconditions.checkArgument(
                  NAME_PATTERN.matcher(name).matches(),
                  "Parameter name must conform to the pattern "
                      + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
            }
            this.name = name;
            return this;
          }

          @Override
          public Start set(String parameterName, Object value) {
            return (Start) super.set(parameterName, value);
          }
        }
      }
    }
  }

  /**
   * Builder for {@link DataStream}.
   *
   * <p>Implementation is not thread-safe.
   *
   * @since 1.3.0
   */
  public static final class Builder
      extends com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient.Builder {

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
          DEFAULT_ROOT_URL,
          DEFAULT_SERVICE_PATH,
          httpRequestInitializer,
          false);
      setBatchPath(DEFAULT_BATCH_PATH);
    }

    /** Builds a new instance of {@link DataStream}. */
    @Override
    public DataStream build() {
      return new DataStream(this);
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
     * Set the {@link DataStreamRequestInitializer}.
     *
     * @since 1.12
     */
    public Builder setDataStreamRequestInitializer(
        DataStreamRequestInitializer datastreamRequestInitializer) {
      return (Builder) super.setGoogleClientRequestInitializer(datastreamRequestInitializer);
    }

    @Override
    public Builder setGoogleClientRequestInitializer(
        com.google.api.client.googleapis.services.GoogleClientRequestInitializer
            googleClientRequestInitializer) {
      return (Builder) super.setGoogleClientRequestInitializer(googleClientRequestInitializer);
    }
  }
}

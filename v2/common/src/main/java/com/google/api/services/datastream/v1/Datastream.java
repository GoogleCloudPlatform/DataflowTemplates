/*
 * Copyright (C) 2022 Google LLC
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
package com.google.api.services.datastream.v1;

/**
 * Service definition for Datastream (v1).
 *
 * <p>
 *
 * <p>For more information about this service, see the <a
 * href="https://cloud.google.com/datastream/" target="_blank">API Documentation</a>
 *
 * <p>This service uses {@link DatastreamRequestInitializer} to initialize global parameters via its
 * {@link Builder}.
 *
 * @since 1.3
 * @author Google, Inc.
 */
@SuppressWarnings("javadoc")
public class Datastream
    extends com.google.api.client.googleapis.services.json.AbstractGoogleJsonClient {

  // Note: Leave this static initializer at the top of the file.
  static {
    com.google.api.client.util.Preconditions.checkState(
        com.google.api.client.googleapis.GoogleUtils.MAJOR_VERSION == 1
            && com.google.api.client.googleapis.GoogleUtils.MINOR_VERSION >= 15,
        "You are currently running with version %s of google-api-client. "
            + "You need at least version 1.15 of google-api-client to run version "
            + "1.25.0-SNAPSHOT of the Datastream API library.",
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
  public Datastream(
      com.google.api.client.http.HttpTransport transport,
      com.google.api.client.json.JsonFactory jsonFactory,
      com.google.api.client.http.HttpRequestInitializer httpRequestInitializer) {
    this(new Builder(transport, jsonFactory, httpRequestInitializer));
  }

  /** @param builder builder */
  Datastream(Builder builder) {
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
   *   {@code Datastream datastream = new Datastream(...);}
   *   {@code Datastream.Projects.List request = datastream.projects().list(parameters ...)}
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
     *   {@code Datastream datastream = new Datastream(...);}
     *   {@code Datastream.Locations.List request = datastream.locations().list(parameters ...)}
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
       * The FetchStaticIps API call exposes the static IP addresses used by Datastream.
       *
       * <p>Create a request for the method "locations.fetchStaticIps".
       *
       * <p>This request holds the parameters needed by the datastream server. After setting any
       * optional parameters, call the {@link FetchStaticIps#execute()} method to invoke the remote
       * operation.
       *
       * @param name Required. The resource name for the location for which static IPs should be
       *     returned. Must be in the format `projects/locations`.
       * @return the request
       */
      public FetchStaticIps fetchStaticIps(java.lang.String name) throws java.io.IOException {
        FetchStaticIps result = new FetchStaticIps(name);
        initialize(result);
        return result;
      }

      public class FetchStaticIps
          extends DatastreamRequest<
              com.google.api.services.datastream.v1.model.FetchStaticIpsResponse> {

        private static final String REST_PATH = "v1/{+name}:fetchStaticIps";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

        /**
         * The FetchStaticIps API call exposes the static IP addresses used by Datastream.
         *
         * <p>Create a request for the method "locations.fetchStaticIps".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link FetchStaticIps#execute()} method to invoke the
         * remote operation.
         *
         * <p>{@link FetchStaticIps#initialize(com.google.api.client.googleapis.services.Abstr
         * actGoogleClientRequest)} must be called to initialize this instance immediately after
         * invoking the constructor.
         *
         * @param name Required. The resource name for the location for which static IPs should be
         *     returned. Must be in the format `projects/locations`.
         * @since 1.13
         */
        protected FetchStaticIps(java.lang.String name) {
          super(
              Datastream.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.datastream.v1.model.FetchStaticIpsResponse.class);
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
         * Required. The resource name for the location for which static IPs should be returned.
         * Must be in the format `projects/locations`.
         */
        @com.google.api.client.util.Key private java.lang.String name;

        /**
         * Required. The resource name for the location for which static IPs should be returned.
         * Must be in the format `projects/locations`.
         */
        public java.lang.String getName() {
          return name;
        }

        /**
         * Required. The resource name for the location for which static IPs should be returned.
         * Must be in the format `projects/locations`.
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
          extends DatastreamRequest<com.google.api.services.datastream.v1.model.Location> {

        private static final String REST_PATH = "v1/{+name}";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

        /**
         * Gets information about a location.
         *
         * <p>Create a request for the method "locations.get".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
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
              Datastream.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.datastream.v1.model.Location.class);
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
          extends DatastreamRequest<
              com.google.api.services.datastream.v1.model.ListLocationsResponse> {

        private static final String REST_PATH = "v1/{+name}/locations";

        private final java.util.regex.Pattern NAME_PATTERN =
            java.util.regex.Pattern.compile("^projects/[^/]+$");

        /**
         * Lists information about the supported locations for this service.
         *
         * <p>Create a request for the method "locations.list".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
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
              Datastream.this,
              "GET",
              REST_PATH,
              null,
              com.google.api.services.datastream.v1.model.ListLocationsResponse.class);
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
         * strings like `"displayName=tokyo"`, and is documented in more detail in
         * [AIP-160](https://google.aip.dev/160).
         */
        @com.google.api.client.util.Key private java.lang.String filter;

        /**
         * A filter to narrow down results to a preferred subset. The filtering language accepts
         * strings like `"displayName=tokyo"`, and is documented in more detail in
         * [AIP-160](https://google.aip.dev/160).
         */
        public java.lang.String getFilter() {
          return filter;
        }

        /**
         * A filter to narrow down results to a preferred subset. The filtering language accepts
         * strings like `"displayName=tokyo"`, and is documented in more detail in
         * [AIP-160](https://google.aip.dev/160).
         */
        public List setFilter(java.lang.String filter) {
          this.filter = filter;
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
         * A page token received from the `next_page_token` field in the response. Send that page
         * token to receive the subsequent page.
         */
        @com.google.api.client.util.Key private java.lang.String pageToken;

        /**
         * A page token received from the `next_page_token` field in the response. Send that page
         * token to receive the subsequent page.
         */
        public java.lang.String getPageToken() {
          return pageToken;
        }

        /**
         * A page token received from the `next_page_token` field in the response. Send that page
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
       * An accessor for creating requests from the ConnectionProfiles collection.
       *
       * <p>The typical use is:
       *
       * <pre>
       *   {@code Datastream datastream = new Datastream(...);}
       *   {@code Datastream.ConnectionProfiles.List request = datastream.connectionProfiles().list(parameters ...)}
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
         * @param parent Required. The parent that owns the collection of ConnectionProfiles.
         * @param content the {@link com.google.api.services.datastream.v1.model.ConnectionProfile}
         * @return the request
         */
        public Create create(
            java.lang.String parent,
            com.google.api.services.datastream.v1.model.ConnectionProfile content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+parent}/connectionProfiles";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to create a connection profile in a project and location.
           *
           * <p>Create a request for the method "connectionProfiles.create".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of ConnectionProfiles.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.ConnectionProfile}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent,
              com.google.api.services.datastream.v1.model.ConnectionProfile content) {
            super(
                Datastream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Required. The parent that owns the collection of ConnectionProfiles. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of ConnectionProfiles. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of ConnectionProfiles. */
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

          /** Required. The connection profile identifier. */
          @com.google.api.client.util.Key private java.lang.String connectionProfileId;

          /** Required. The connection profile identifier. */
          public java.lang.String getConnectionProfileId() {
            return connectionProfileId;
          }

          /** Required. The connection profile identifier. */
          public Create setConnectionProfileId(java.lang.String connectionProfileId) {
            this.connectionProfileId = connectionProfileId;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * Optional. Only validate the connection profile, but don't create any resources. The
           * default is false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the connection profile, but don't create any resources. The
           * default is false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the connection profile, but don't create any resources. The
           * default is false.
           */
          public Create setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          /** Optional. Create the connection profile without validating it. */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /** Optional. Create the connection profile without validating it. */
          public java.lang.Boolean getForce() {
            return force;
          }

          /** Optional. Create the connection profile without validating it. */
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
         * Use this method to delete a connection profile.
         *
         * <p>Create a request for the method "connectionProfiles.delete".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the connection profile resource to delete.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");

          /**
           * Use this method to delete a connection profile.
           *
           * <p>Create a request for the method "connectionProfiles.delete".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the connection profile resource to delete.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                Datastream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Required. The name of the connection profile resource to delete. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the connection profile resource to delete. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the connection profile resource to delete. */
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
         * objects of a parent data object that's optionally supplied in the request.
         *
         * <p>Create a request for the method "connectionProfiles.discover".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Discover#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent resource of the connection profile type. Must be in
         *     the format `projects/locations`.
         * @param content the {@link
         *     com.google.api.services.datastream.v1.model.DiscoverConnectionProfileRequest}
         * @return the request
         */
        public Discover discover(
            java.lang.String parent,
            com.google.api.services.datastream.v1.model.DiscoverConnectionProfileRequest content)
            throws java.io.IOException {
          Discover result = new Discover(parent, content);
          initialize(result);
          return result;
        }

        public class Discover
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.DiscoverConnectionProfileResponse> {

          private static final String REST_PATH = "v1/{+parent}/connectionProfiles:discover";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to discover a connection profile. The discover API call exposes the
           * data objects and metadata belonging to the profile. Typically, a request returns
           * children data objects of a parent data object that's optionally supplied in the
           * request.
           *
           * <p>Create a request for the method "connectionProfiles.discover".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Discover#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Discover#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent resource of the connection profile type. Must be in
           *     the format `projects/locations`.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.DiscoverConnectionProfileRequest}
           * @since 1.13
           */
          protected Discover(
              java.lang.String parent,
              com.google.api.services.datastream.v1.model.DiscoverConnectionProfileRequest
                  content) {
            super(
                Datastream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.DiscoverConnectionProfileResponse
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
           * Required. The parent resource of the connection profile type. Must be in the format
           * `projects/locations`.
           */
          @com.google.api.client.util.Key private java.lang.String parent;

          /**
           * Required. The parent resource of the connection profile type. Must be in the format
           * `projects/locations`.
           */
          public java.lang.String getParent() {
            return parent;
          }

          /**
           * Required. The parent resource of the connection profile type. Must be in the format
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
         * @param name Required. The name of the connection profile resource to get.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.ConnectionProfile> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");

          /**
           * Use this method to get details about a connection profile.
           *
           * <p>Create a request for the method "connectionProfiles.get".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the connection profile resource to get.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.ConnectionProfile.class);
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

          /** Required. The name of the connection profile resource to get. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the connection profile resource to get. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the connection profile resource to get. */
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
         * @param parent Required. The parent that owns the collection of connection profiles.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.ListConnectionProfilesResponse> {

          private static final String REST_PATH = "v1/{+parent}/connectionProfiles";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to list connection profiles created in a project and location.
           *
           * <p>Create a request for the method "connectionProfiles.list".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of connection profiles.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.ListConnectionProfilesResponse.class);
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

          /** Required. The parent that owns the collection of connection profiles. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of connection profiles. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of connection profiles. */
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
           * Maximum number of connection profiles to return. If unspecified, at most 50 connection
           * profiles will be returned. The maximum value is 1000; values above 1000 will be coerced
           * to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Maximum number of connection profiles to return. If unspecified, at most 50 connection
           * profiles will be returned. The maximum value is 1000; values above 1000 will be coerced
           * to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Maximum number of connection profiles to return. If unspecified, at most 50 connection
           * profiles will be returned. The maximum value is 1000; values above 1000 will be coerced
           * to 1000.
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
         * @param content the {@link com.google.api.services.datastream.v1.model.ConnectionProfile}
         * @return the request
         */
        public Patch patch(
            java.lang.String name,
            com.google.api.services.datastream.v1.model.ConnectionProfile content)
            throws java.io.IOException {
          Patch result = new Patch(name, content);
          initialize(result);
          return result;
        }

        public class Patch
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/connectionProfiles/[^/]+$");

          /**
           * Use this method to update the parameters of a connection profile.
           *
           * <p>Create a request for the method "connectionProfiles.patch".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Output only. The resource's name.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.ConnectionProfile}
           * @since 1.13
           */
          protected Patch(
              java.lang.String name,
              com.google.api.services.datastream.v1.model.ConnectionProfile content) {
            super(
                Datastream.this,
                "PATCH",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.Operation.class);
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
           * Optional. Field mask is used to specify the fields to be overwritten in the
           * ConnectionProfile resource by the update. The fields specified in the update_mask are
           * relative to the resource, not the full request. A field will be overwritten if it is in
           * the mask. If the user does not provide a mask then all fields will be overwritten.
           */
          @com.google.api.client.util.Key private String updateMask;

          /**
           * Optional. Field mask is used to specify the fields to be overwritten in the
           * ConnectionProfile resource by the update. The fields specified in the update_mask are
           * relative to the resource, not the full request. A field will be overwritten if it is in
           * the mask. If the user does not provide a mask then all fields will be overwritten.
           */
          public String getUpdateMask() {
            return updateMask;
          }

          /**
           * Optional. Field mask is used to specify the fields to be overwritten in the
           * ConnectionProfile resource by the update. The fields specified in the update_mask are
           * relative to the resource, not the full request. A field will be overwritten if it is in
           * the mask. If the user does not provide a mask then all fields will be overwritten.
           */
          public Patch setUpdateMask(String updateMask) {
            this.updateMask = updateMask;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * Optional. Only validate the connection profile, but don't update any resources. The
           * default is false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the connection profile, but don't update any resources. The
           * default is false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the connection profile, but don't update any resources. The
           * default is false.
           */
          public Patch setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          /** Optional. Update the connection profile without validating it. */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /** Optional. Update the connection profile without validating it. */
          public java.lang.Boolean getForce() {
            return force;
          }

          /** Optional. Update the connection profile without validating it. */
          public Patch setForce(java.lang.Boolean force) {
            this.force = force;
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
       *   {@code Datastream datastream = new Datastream(...);}
       *   {@code Datastream.Operations.List request = datastream.operations().list(parameters ...)}
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
         *     com.google.api.services.datastream.v1.model.CancelOperationRequest}
         * @return the request
         */
        public Cancel cancel(
            java.lang.String name,
            com.google.api.services.datastream.v1.model.CancelOperationRequest content)
            throws java.io.IOException {
          Cancel result = new Cancel(name, content);
          initialize(result);
          return result;
        }

        public class Cancel
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Empty> {

          private static final String REST_PATH = "v1/{+name}:cancel";

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
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Cancel#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Cancel#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource to be cancelled.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.CancelOperationRequest}
           * @since 1.13
           */
          protected Cancel(
              java.lang.String name,
              com.google.api.services.datastream.v1.model.CancelOperationRequest content) {
            super(
                Datastream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.Empty.class);
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
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Empty> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

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
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource to be deleted.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                Datastream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.Empty.class);
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
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/operations/[^/]+$");

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
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name The name of the operation resource.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.Operation.class);
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
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.ListOperationsResponse> {

          private static final String REST_PATH = "v1/{+name}/operations";

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
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
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
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.ListOperationsResponse.class);
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
       *   {@code Datastream datastream = new Datastream(...);}
       *   {@code Datastream.PrivateConnections.List request = datastream.privateConnections().list(parameters ...)}
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
         * Use this method to create a private connectivity configuration.
         *
         * <p>Create a request for the method "privateConnections.create".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Create#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent that owns the collection of PrivateConnections.
         * @param content the {@link com.google.api.services.datastream.v1.model.PrivateConnection}
         * @return the request
         */
        public Create create(
            java.lang.String parent,
            com.google.api.services.datastream.v1.model.PrivateConnection content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+parent}/privateConnections";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to create a private connectivity configuration.
           *
           * <p>Create a request for the method "privateConnections.create".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of PrivateConnections.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.PrivateConnection}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent,
              com.google.api.services.datastream.v1.model.PrivateConnection content) {
            super(
                Datastream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Required. The private connectivity identifier. */
          @com.google.api.client.util.Key private java.lang.String privateConnectionId;

          /** Required. The private connectivity identifier. */
          public java.lang.String getPrivateConnectionId() {
            return privateConnectionId;
          }

          /** Required. The private connectivity identifier. */
          public Create setPrivateConnectionId(java.lang.String privateConnectionId) {
            this.privateConnectionId = privateConnectionId;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
         * Use this method to delete a private connectivity configuration.
         *
         * <p>Create a request for the method "privateConnections.delete".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Delete#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the private connectivity configuration to delete.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

          /**
           * Use this method to delete a private connectivity configuration.
           *
           * <p>Create a request for the method "privateConnections.delete".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the private connectivity configuration to delete.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                Datastream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Required. The name of the private connectivity configuration to delete. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the private connectivity configuration to delete. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the private connectivity configuration to delete. */
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
         * Use this method to get details about a private connectivity configuration.
         *
         * <p>Create a request for the method "privateConnections.get".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the private connectivity configuration to get.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.PrivateConnection> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile(
                  "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

          /**
           * Use this method to get details about a private connectivity configuration.
           *
           * <p>Create a request for the method "privateConnections.get".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the private connectivity configuration to get.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.PrivateConnection.class);
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

          /** Required. The name of the private connectivity configuration to get. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the private connectivity configuration to get. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the private connectivity configuration to get. */
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
         * Use this method to list private connectivity configurations in a project and location.
         *
         * <p>Create a request for the method "privateConnections.list".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link List#execute()} method to invoke the remote
         * operation.
         *
         * @param parent Required. The parent that owns the collection of private connectivity
         *     configurations.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.ListPrivateConnectionsResponse> {

          private static final String REST_PATH = "v1/{+parent}/privateConnections";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to list private connectivity configurations in a project and location.
           *
           * <p>Create a request for the method "privateConnections.list".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of private connectivity
           *     configurations.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.ListPrivateConnectionsResponse.class);
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
           * Required. The parent that owns the collection of private connectivity configurations.
           */
          @com.google.api.client.util.Key private java.lang.String parent;

          /**
           * Required. The parent that owns the collection of private connectivity configurations.
           */
          public java.lang.String getParent() {
            return parent;
          }

          /**
           * Required. The parent that owns the collection of private connectivity configurations.
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

          /**
           * Maximum number of private connectivity configurations to return. If unspecified, at
           * most 50 private connectivity configurations that will be returned. The maximum value is
           * 1000; values above 1000 will be coerced to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Maximum number of private connectivity configurations to return. If unspecified, at
           * most 50 private connectivity configurations that will be returned. The maximum value is
           * 1000; values above 1000 will be coerced to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Maximum number of private connectivity configurations to return. If unspecified, at
           * most 50 private connectivity configurations that will be returned. The maximum value is
           * 1000; values above 1000 will be coerced to 1000.
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
         *   {@code Datastream datastream = new Datastream(...);}
         *   {@code Datastream.Routes.List request = datastream.routes().list(parameters ...)}
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
           * Use this method to create a route for a private connectivity configuration in a project
           * and location.
           *
           * <p>Create a request for the method "routes.create".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The parent that owns the collection of Routes.
           * @param content the {@link com.google.api.services.datastream.v1.model.Route}
           * @return the request
           */
          public Create create(
              java.lang.String parent, com.google.api.services.datastream.v1.model.Route content)
              throws java.io.IOException {
            Create result = new Create(parent, content);
            initialize(result);
            return result;
          }

          public class Create
              extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

            private static final String REST_PATH = "v1/{+parent}/routes";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

            /**
             * Use this method to create a route for a private connectivity configuration in a
             * project and location.
             *
             * <p>Create a request for the method "routes.create".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link Create#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The parent that owns the collection of Routes.
             * @param content the {@link com.google.api.services.datastream.v1.model.Route}
             * @since 1.13
             */
            protected Create(
                java.lang.String parent,
                com.google.api.services.datastream.v1.model.Route content) {
              super(
                  Datastream.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.datastream.v1.model.Operation.class);
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
             * request and the request times out. If you make the request again with the same
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
             * request and the request times out. If you make the request again with the same
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
             * request and the request times out. If you make the request again with the same
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
              extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");

            /**
             * Use this method to delete a route.
             *
             * <p>Create a request for the method "routes.delete".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link Delete#execute()} method to invoke the
             * remote operation.
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
                  Datastream.this,
                  "DELETE",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1.model.Operation.class);
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
             * request and the request times out. If you make the request again with the same
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
             * request and the request times out. If you make the request again with the same
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
             * request and the request times out. If you make the request again with the same
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
              extends DatastreamRequest<com.google.api.services.datastream.v1.model.Route> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+/routes/[^/]+$");

            /**
             * Use this method to get details about a route.
             *
             * <p>Create a request for the method "routes.get".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
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
                  Datastream.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1.model.Route.class);
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
           * Use this method to list routes created for a private connectivity configuration in a
           * project and location.
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
              extends DatastreamRequest<
                  com.google.api.services.datastream.v1.model.ListRoutesResponse> {

            private static final String REST_PATH = "v1/{+parent}/routes";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/privateConnections/[^/]+$");

            /**
             * Use this method to list routes created for a private connectivity configuration in a
             * project and location.
             *
             * <p>Create a request for the method "routes.list".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
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
                  Datastream.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1.model.ListRoutesResponse.class);
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
       *   {@code Datastream datastream = new Datastream(...);}
       *   {@code Datastream.Streams.List request = datastream.streams().list(parameters ...)}
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
         * @param parent Required. The parent that owns the collection of streams.
         * @param content the {@link com.google.api.services.datastream.v1.model.Stream}
         * @return the request
         */
        public Create create(
            java.lang.String parent, com.google.api.services.datastream.v1.model.Stream content)
            throws java.io.IOException {
          Create result = new Create(parent, content);
          initialize(result);
          return result;
        }

        public class Create
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+parent}/streams";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to create a stream.
           *
           * <p>Create a request for the method "streams.create".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Create#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Create#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of streams.
           * @param content the {@link com.google.api.services.datastream.v1.model.Stream}
           * @since 1.13
           */
          protected Create(
              java.lang.String parent, com.google.api.services.datastream.v1.model.Stream content) {
            super(
                Datastream.this,
                "POST",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Required. The parent that owns the collection of streams. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of streams. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of streams. */
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

          /** Required. The stream identifier. */
          @com.google.api.client.util.Key private java.lang.String streamId;

          /** Required. The stream identifier. */
          public java.lang.String getStreamId() {
            return streamId;
          }

          /** Required. The stream identifier. */
          public Create setStreamId(java.lang.String streamId) {
            this.streamId = streamId;
            return this;
          }

          /**
           * Optional. A request ID to identify requests. Specify a unique request ID so that if you
           * must retry your request, the server will know to ignore the request if it has already
           * been completed. The server will guarantee that for at least 60 minutes since the first
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * Optional. Only validate the stream, but don't create any resources. The default is
           * false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the stream, but don't create any resources. The default is
           * false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the stream, but don't create any resources. The default is
           * false.
           */
          public Create setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          /** Optional. Create the stream without validating it. */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /** Optional. Create the stream without validating it. */
          public java.lang.Boolean getForce() {
            return force;
          }

          /** Optional. Create the stream without validating it. */
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
         * @param name Required. The name of the stream resource to delete.
         * @return the request
         */
        public Delete delete(java.lang.String name) throws java.io.IOException {
          Delete result = new Delete(name);
          initialize(result);
          return result;
        }

        public class Delete
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to delete a stream.
           *
           * <p>Create a request for the method "streams.delete".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Delete#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Delete#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the stream resource to delete.
           * @since 1.13
           */
          protected Delete(java.lang.String name) {
            super(
                Datastream.this,
                "DELETE",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Required. The name of the stream resource to delete. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the stream resource to delete. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the stream resource to delete. */
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
         * Use this method to get details about a stream.
         *
         * <p>Create a request for the method "streams.get".
         *
         * <p>This request holds the parameters needed by the datastream server. After setting any
         * optional parameters, call the {@link Get#execute()} method to invoke the remote
         * operation.
         *
         * @param name Required. The name of the stream resource to get.
         * @return the request
         */
        public Get get(java.lang.String name) throws java.io.IOException {
          Get result = new Get(name);
          initialize(result);
          return result;
        }

        public class Get
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Stream> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to get details about a stream.
           *
           * <p>Create a request for the method "streams.get".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Required. The name of the stream resource to get.
           * @since 1.13
           */
          protected Get(java.lang.String name) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.Stream.class);
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

          /** Required. The name of the stream resource to get. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Required. The name of the stream resource to get. */
          public java.lang.String getName() {
            return name;
          }

          /** Required. The name of the stream resource to get. */
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
         * @param parent Required. The parent that owns the collection of streams.
         * @return the request
         */
        public List list(java.lang.String parent) throws java.io.IOException {
          List result = new List(parent);
          initialize(result);
          return result;
        }

        public class List
            extends DatastreamRequest<
                com.google.api.services.datastream.v1.model.ListStreamsResponse> {

          private static final String REST_PATH = "v1/{+parent}/streams";

          private final java.util.regex.Pattern PARENT_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+$");

          /**
           * Use this method to list streams in a project and location.
           *
           * <p>Create a request for the method "streams.list".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param parent Required. The parent that owns the collection of streams.
           * @since 1.13
           */
          protected List(java.lang.String parent) {
            super(
                Datastream.this,
                "GET",
                REST_PATH,
                null,
                com.google.api.services.datastream.v1.model.ListStreamsResponse.class);
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

          /** Required. The parent that owns the collection of streams. */
          @com.google.api.client.util.Key private java.lang.String parent;

          /** Required. The parent that owns the collection of streams. */
          public java.lang.String getParent() {
            return parent;
          }

          /** Required. The parent that owns the collection of streams. */
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
           * Maximum number of streams to return. If unspecified, at most 50 streams will be
           * returned. The maximum value is 1000; values above 1000 will be coerced to 1000.
           */
          @com.google.api.client.util.Key private java.lang.Integer pageSize;

          /**
           * Maximum number of streams to return. If unspecified, at most 50 streams will be
           * returned. The maximum value is 1000; values above 1000 will be coerced to 1000.
           */
          public java.lang.Integer getPageSize() {
            return pageSize;
          }

          /**
           * Maximum number of streams to return. If unspecified, at most 50 streams will be
           * returned. The maximum value is 1000; values above 1000 will be coerced to 1000.
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
         * @param name Output only. The stream's name.
         * @param content the {@link com.google.api.services.datastream.v1.model.Stream}
         * @return the request
         */
        public Patch patch(
            java.lang.String name, com.google.api.services.datastream.v1.model.Stream content)
            throws java.io.IOException {
          Patch result = new Patch(name, content);
          initialize(result);
          return result;
        }

        public class Patch
            extends DatastreamRequest<com.google.api.services.datastream.v1.model.Operation> {

          private static final String REST_PATH = "v1/{+name}";

          private final java.util.regex.Pattern NAME_PATTERN =
              java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

          /**
           * Use this method to update the configuration of a stream.
           *
           * <p>Create a request for the method "streams.patch".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Patch#execute()} method to invoke the remote
           * operation.
           *
           * <p>{@link
           * Patch#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
           * must be called to initialize this instance immediately after invoking the constructor.
           *
           * @param name Output only. The stream's name.
           * @param content the {@link com.google.api.services.datastream.v1.model.Stream}
           * @since 1.13
           */
          protected Patch(
              java.lang.String name, com.google.api.services.datastream.v1.model.Stream content) {
            super(
                Datastream.this,
                "PATCH",
                REST_PATH,
                content,
                com.google.api.services.datastream.v1.model.Operation.class);
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

          /** Output only. The stream's name. */
          @com.google.api.client.util.Key private java.lang.String name;

          /** Output only. The stream's name. */
          public java.lang.String getName() {
            return name;
          }

          /** Output only. The stream's name. */
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
           * Optional. Field mask is used to specify the fields to be overwritten in the stream
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          @com.google.api.client.util.Key private String updateMask;

          /**
           * Optional. Field mask is used to specify the fields to be overwritten in the stream
           * resource by the update. The fields specified in the update_mask are relative to the
           * resource, not the full request. A field will be overwritten if it is in the mask. If
           * the user does not provide a mask then all fields will be overwritten.
           */
          public String getUpdateMask() {
            return updateMask;
          }

          /**
           * Optional. Field mask is used to specify the fields to be overwritten in the stream
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * request. For example, consider a situation where you make an initial request and the
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
           * Optional. Only validate the stream with the changes, without actually updating it. The
           * default is false.
           */
          @com.google.api.client.util.Key private java.lang.Boolean validateOnly;

          /**
           * Optional. Only validate the stream with the changes, without actually updating it. The
           * default is false.
           */
          public java.lang.Boolean getValidateOnly() {
            return validateOnly;
          }

          /**
           * Optional. Only validate the stream with the changes, without actually updating it. The
           * default is false.
           */
          public Patch setValidateOnly(java.lang.Boolean validateOnly) {
            this.validateOnly = validateOnly;
            return this;
          }

          /** Optional. Update the stream without validating it. */
          @com.google.api.client.util.Key private java.lang.Boolean force;

          /** Optional. Update the stream without validating it. */
          public java.lang.Boolean getForce() {
            return force;
          }

          /** Optional. Update the stream without validating it. */
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
         * An accessor for creating requests from the Objects collection.
         *
         * <p>The typical use is:
         *
         * <pre>
         *   {@code Datastream datastream = new Datastream(...);}
         *   {@code Datastream.Objects.List request = datastream.objects().list(parameters ...)}
         * </pre>
         *
         * @return the resource collection
         */
        public Objects objects() {
          return new Objects();
        }

        /** The "objects" collection of methods. */
        public class Objects {

          /**
           * Use this method to get details about a stream object.
           *
           * <p>Create a request for the method "objects.get".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Get#execute()} method to invoke the remote
           * operation.
           *
           * @param name Required. The name of the stream object resource to get.
           * @return the request
           */
          public Get get(java.lang.String name) throws java.io.IOException {
            Get result = new Get(name);
            initialize(result);
            return result;
          }

          public class Get
              extends DatastreamRequest<com.google.api.services.datastream.v1.model.StreamObject> {

            private static final String REST_PATH = "v1/{+name}";

            private final java.util.regex.Pattern NAME_PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");

            /**
             * Use this method to get details about a stream object.
             *
             * <p>Create a request for the method "objects.get".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link Get#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * Get#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param name Required. The name of the stream object resource to get.
             * @since 1.13
             */
            protected Get(java.lang.String name) {
              super(
                  Datastream.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1.model.StreamObject.class);
              this.name =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      name, "Required parameter name must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");
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

            /** Required. The name of the stream object resource to get. */
            @com.google.api.client.util.Key private java.lang.String name;

            /** Required. The name of the stream object resource to get. */
            public java.lang.String getName() {
              return name;
            }

            /** Required. The name of the stream object resource to get. */
            public Get setName(java.lang.String name) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    NAME_PATTERN.matcher(name).matches(),
                    "Parameter name must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");
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
           * Use this method to list the objects of a specific stream.
           *
           * <p>Create a request for the method "objects.list".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link List#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The parent stream that owns the collection of objects.
           * @return the request
           */
          public List list(java.lang.String parent) throws java.io.IOException {
            List result = new List(parent);
            initialize(result);
            return result;
          }

          public class List
              extends DatastreamRequest<
                  com.google.api.services.datastream.v1.model.ListStreamObjectsResponse> {

            private static final String REST_PATH = "v1/{+parent}/objects";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

            /**
             * Use this method to list the objects of a specific stream.
             *
             * <p>Create a request for the method "objects.list".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link List#execute()} method to invoke the remote
             * operation.
             *
             * <p>{@link
             * List#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The parent stream that owns the collection of objects.
             * @since 1.13
             */
            protected List(java.lang.String parent) {
              super(
                  Datastream.this,
                  "GET",
                  REST_PATH,
                  null,
                  com.google.api.services.datastream.v1.model.ListStreamObjectsResponse.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
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

            /** Required. The parent stream that owns the collection of objects. */
            @com.google.api.client.util.Key private java.lang.String parent;

            /** Required. The parent stream that owns the collection of objects. */
            public java.lang.String getParent() {
              return parent;
            }

            /** Required. The parent stream that owns the collection of objects. */
            public List setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            /**
             * Maximum number of objects to return. Default is 50. The maximum value is 1000; values
             * above 1000 will be coerced to 1000.
             */
            @com.google.api.client.util.Key private java.lang.Integer pageSize;

            /**
             * Maximum number of objects to return. Default is 50. The maximum value is 1000; values
             * above 1000 will be coerced to 1000.
             */
            public java.lang.Integer getPageSize() {
              return pageSize;
            }

            /**
             * Maximum number of objects to return. Default is 50. The maximum value is 1000; values
             * above 1000 will be coerced to 1000.
             */
            public List setPageSize(java.lang.Integer pageSize) {
              this.pageSize = pageSize;
              return this;
            }

            /**
             * Page token received from a previous `ListStreamObjectsRequest` call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * `ListStreamObjectsRequest` must match the call that provided the page token.
             */
            @com.google.api.client.util.Key private java.lang.String pageToken;

            /**
             * Page token received from a previous `ListStreamObjectsRequest` call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * `ListStreamObjectsRequest` must match the call that provided the page token.
             */
            public java.lang.String getPageToken() {
              return pageToken;
            }

            /**
             * Page token received from a previous `ListStreamObjectsRequest` call. Provide this to
             * retrieve the subsequent page. When paginating, all other parameters provided to
             * `ListStreamObjectsRequest` must match the call that provided the page token.
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
           * Use this method to look up a stream object by its source object identifier.
           *
           * <p>Create a request for the method "objects.lookup".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link Lookup#execute()} method to invoke the remote
           * operation.
           *
           * @param parent Required. The parent stream that owns the collection of objects.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.LookupStreamObjectRequest}
           * @return the request
           */
          public Lookup lookup(
              java.lang.String parent,
              com.google.api.services.datastream.v1.model.LookupStreamObjectRequest content)
              throws java.io.IOException {
            Lookup result = new Lookup(parent, content);
            initialize(result);
            return result;
          }

          public class Lookup
              extends DatastreamRequest<com.google.api.services.datastream.v1.model.StreamObject> {

            private static final String REST_PATH = "v1/{+parent}/objects:lookup";

            private final java.util.regex.Pattern PARENT_PATTERN =
                java.util.regex.Pattern.compile("^projects/[^/]+/locations/[^/]+/streams/[^/]+$");

            /**
             * Use this method to look up a stream object by its source object identifier.
             *
             * <p>Create a request for the method "objects.lookup".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link Lookup#execute()} method to invoke the
             * remote operation.
             *
             * <p>{@link
             * Lookup#initialize(com.google.api.client.googleapis.services.AbstractGoogleClientRequest)}
             * must be called to initialize this instance immediately after invoking the
             * constructor.
             *
             * @param parent Required. The parent stream that owns the collection of objects.
             * @param content the {@link
             *     com.google.api.services.datastream.v1.model.LookupStreamObjectRequest}
             * @since 1.13
             */
            protected Lookup(
                java.lang.String parent,
                com.google.api.services.datastream.v1.model.LookupStreamObjectRequest content) {
              super(
                  Datastream.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.datastream.v1.model.StreamObject.class);
              this.parent =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      parent, "Required parameter parent must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
              }
            }

            @Override
            public Lookup set$Xgafv(java.lang.String $Xgafv) {
              return (Lookup) super.set$Xgafv($Xgafv);
            }

            @Override
            public Lookup setAccessToken(java.lang.String accessToken) {
              return (Lookup) super.setAccessToken(accessToken);
            }

            @Override
            public Lookup setAlt(java.lang.String alt) {
              return (Lookup) super.setAlt(alt);
            }

            @Override
            public Lookup setCallback(java.lang.String callback) {
              return (Lookup) super.setCallback(callback);
            }

            @Override
            public Lookup setFields(java.lang.String fields) {
              return (Lookup) super.setFields(fields);
            }

            @Override
            public Lookup setKey(java.lang.String key) {
              return (Lookup) super.setKey(key);
            }

            @Override
            public Lookup setOauthToken(java.lang.String oauthToken) {
              return (Lookup) super.setOauthToken(oauthToken);
            }

            @Override
            public Lookup setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (Lookup) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public Lookup setQuotaUser(java.lang.String quotaUser) {
              return (Lookup) super.setQuotaUser(quotaUser);
            }

            @Override
            public Lookup setUploadType(java.lang.String uploadType) {
              return (Lookup) super.setUploadType(uploadType);
            }

            @Override
            public Lookup setUploadProtocol(java.lang.String uploadProtocol) {
              return (Lookup) super.setUploadProtocol(uploadProtocol);
            }

            /** Required. The parent stream that owns the collection of objects. */
            @com.google.api.client.util.Key private java.lang.String parent;

            /** Required. The parent stream that owns the collection of objects. */
            public java.lang.String getParent() {
              return parent;
            }

            /** Required. The parent stream that owns the collection of objects. */
            public Lookup setParent(java.lang.String parent) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    PARENT_PATTERN.matcher(parent).matches(),
                    "Parameter parent must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+$");
              }
              this.parent = parent;
              return this;
            }

            @Override
            public Lookup set(String parameterName, Object value) {
              return (Lookup) super.set(parameterName, value);
            }
          }
          /**
           * Use this method to start a backfill job for the specified stream object.
           *
           * <p>Create a request for the method "objects.startBackfillJob".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link StartBackfillJob#execute()} method to invoke the
           * remote operation.
           *
           * @param object__ Required. The name of the stream object resource to start a backfill
           *     job for.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.StartBackfillJobRequest}
           * @return the request
           */
          public StartBackfillJob startBackfillJob(
              java.lang.String object__,
              com.google.api.services.datastream.v1.model.StartBackfillJobRequest content)
              throws java.io.IOException {
            StartBackfillJob result = new StartBackfillJob(object__, content);
            initialize(result);
            return result;
          }

          public class StartBackfillJob
              extends DatastreamRequest<
                  com.google.api.services.datastream.v1.model.StartBackfillJobResponse> {

            private static final String REST_PATH = "v1/{+object}:startBackfillJob";

            private final java.util.regex.Pattern OBJECT___PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");

            /**
             * Use this method to start a backfill job for the specified stream object.
             *
             * <p>Create a request for the method "objects.startBackfillJob".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link StartBackfillJob#execute()} method to invoke
             * the remote operation.
             *
             * <p>{@link StartBackfillJob#initialize(com.google.api.client.googleapis.services.Abs
             * tractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param object__ Required. The name of the stream object resource to start a backfill
             *     job for.
             * @param content the {@link
             *     com.google.api.services.datastream.v1.model.StartBackfillJobRequest}
             * @since 1.13
             */
            protected StartBackfillJob(
                java.lang.String object__,
                com.google.api.services.datastream.v1.model.StartBackfillJobRequest content) {
              super(
                  Datastream.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.datastream.v1.model.StartBackfillJobResponse.class);
              this.object__ =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      object__, "Required parameter object__ must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    OBJECT___PATTERN.matcher(object__).matches(),
                    "Parameter object__ must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");
              }
            }

            @Override
            public StartBackfillJob set$Xgafv(java.lang.String $Xgafv) {
              return (StartBackfillJob) super.set$Xgafv($Xgafv);
            }

            @Override
            public StartBackfillJob setAccessToken(java.lang.String accessToken) {
              return (StartBackfillJob) super.setAccessToken(accessToken);
            }

            @Override
            public StartBackfillJob setAlt(java.lang.String alt) {
              return (StartBackfillJob) super.setAlt(alt);
            }

            @Override
            public StartBackfillJob setCallback(java.lang.String callback) {
              return (StartBackfillJob) super.setCallback(callback);
            }

            @Override
            public StartBackfillJob setFields(java.lang.String fields) {
              return (StartBackfillJob) super.setFields(fields);
            }

            @Override
            public StartBackfillJob setKey(java.lang.String key) {
              return (StartBackfillJob) super.setKey(key);
            }

            @Override
            public StartBackfillJob setOauthToken(java.lang.String oauthToken) {
              return (StartBackfillJob) super.setOauthToken(oauthToken);
            }

            @Override
            public StartBackfillJob setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (StartBackfillJob) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public StartBackfillJob setQuotaUser(java.lang.String quotaUser) {
              return (StartBackfillJob) super.setQuotaUser(quotaUser);
            }

            @Override
            public StartBackfillJob setUploadType(java.lang.String uploadType) {
              return (StartBackfillJob) super.setUploadType(uploadType);
            }

            @Override
            public StartBackfillJob setUploadProtocol(java.lang.String uploadProtocol) {
              return (StartBackfillJob) super.setUploadProtocol(uploadProtocol);
            }

            /** Required. The name of the stream object resource to start a backfill job for. */
            @com.google.api.client.util.Key("object")
            private java.lang.String object__;

            /** Required. The name of the stream object resource to start a backfill job for. */
            public java.lang.String getObject() {
              return object__;
            }

            /** Required. The name of the stream object resource to start a backfill job for. */
            public StartBackfillJob setObject(java.lang.String object__) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    OBJECT___PATTERN.matcher(object__).matches(),
                    "Parameter object__ must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");
              }
              this.object__ = object__;
              return this;
            }

            @Override
            public StartBackfillJob set(String parameterName, Object value) {
              return (StartBackfillJob) super.set(parameterName, value);
            }
          }
          /**
           * Use this method to stop a backfill job for the specified stream object.
           *
           * <p>Create a request for the method "objects.stopBackfillJob".
           *
           * <p>This request holds the parameters needed by the datastream server. After setting any
           * optional parameters, call the {@link StopBackfillJob#execute()} method to invoke the
           * remote operation.
           *
           * @param object__ Required. The name of the stream object resource to stop the backfill
           *     job for.
           * @param content the {@link
           *     com.google.api.services.datastream.v1.model.StopBackfillJobRequest}
           * @return the request
           */
          public StopBackfillJob stopBackfillJob(
              java.lang.String object__,
              com.google.api.services.datastream.v1.model.StopBackfillJobRequest content)
              throws java.io.IOException {
            StopBackfillJob result = new StopBackfillJob(object__, content);
            initialize(result);
            return result;
          }

          public class StopBackfillJob
              extends DatastreamRequest<
                  com.google.api.services.datastream.v1.model.StopBackfillJobResponse> {

            private static final String REST_PATH = "v1/{+object}:stopBackfillJob";

            private final java.util.regex.Pattern OBJECT___PATTERN =
                java.util.regex.Pattern.compile(
                    "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");

            /**
             * Use this method to stop a backfill job for the specified stream object.
             *
             * <p>Create a request for the method "objects.stopBackfillJob".
             *
             * <p>This request holds the parameters needed by the datastream server. After setting
             * any optional parameters, call the {@link StopBackfillJob#execute()} method to invoke
             * the remote operation.
             *
             * <p>{@link StopBackfillJob#initialize(com.google.api.client.googleapis.services.Abst
             * ractGoogleClientRequest)} must be called to initialize this instance immediately
             * after invoking the constructor.
             *
             * @param object__ Required. The name of the stream object resource to stop the backfill
             *     job for.
             * @param content the {@link
             *     com.google.api.services.datastream.v1.model.StopBackfillJobRequest}
             * @since 1.13
             */
            protected StopBackfillJob(
                java.lang.String object__,
                com.google.api.services.datastream.v1.model.StopBackfillJobRequest content) {
              super(
                  Datastream.this,
                  "POST",
                  REST_PATH,
                  content,
                  com.google.api.services.datastream.v1.model.StopBackfillJobResponse.class);
              this.object__ =
                  com.google.api.client.util.Preconditions.checkNotNull(
                      object__, "Required parameter object__ must be specified.");
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    OBJECT___PATTERN.matcher(object__).matches(),
                    "Parameter object__ must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");
              }
            }

            @Override
            public StopBackfillJob set$Xgafv(java.lang.String $Xgafv) {
              return (StopBackfillJob) super.set$Xgafv($Xgafv);
            }

            @Override
            public StopBackfillJob setAccessToken(java.lang.String accessToken) {
              return (StopBackfillJob) super.setAccessToken(accessToken);
            }

            @Override
            public StopBackfillJob setAlt(java.lang.String alt) {
              return (StopBackfillJob) super.setAlt(alt);
            }

            @Override
            public StopBackfillJob setCallback(java.lang.String callback) {
              return (StopBackfillJob) super.setCallback(callback);
            }

            @Override
            public StopBackfillJob setFields(java.lang.String fields) {
              return (StopBackfillJob) super.setFields(fields);
            }

            @Override
            public StopBackfillJob setKey(java.lang.String key) {
              return (StopBackfillJob) super.setKey(key);
            }

            @Override
            public StopBackfillJob setOauthToken(java.lang.String oauthToken) {
              return (StopBackfillJob) super.setOauthToken(oauthToken);
            }

            @Override
            public StopBackfillJob setPrettyPrint(java.lang.Boolean prettyPrint) {
              return (StopBackfillJob) super.setPrettyPrint(prettyPrint);
            }

            @Override
            public StopBackfillJob setQuotaUser(java.lang.String quotaUser) {
              return (StopBackfillJob) super.setQuotaUser(quotaUser);
            }

            @Override
            public StopBackfillJob setUploadType(java.lang.String uploadType) {
              return (StopBackfillJob) super.setUploadType(uploadType);
            }

            @Override
            public StopBackfillJob setUploadProtocol(java.lang.String uploadProtocol) {
              return (StopBackfillJob) super.setUploadProtocol(uploadProtocol);
            }

            /** Required. The name of the stream object resource to stop the backfill job for. */
            @com.google.api.client.util.Key("object")
            private java.lang.String object__;

            /** Required. The name of the stream object resource to stop the backfill job for. */
            public java.lang.String getObject() {
              return object__;
            }

            /** Required. The name of the stream object resource to stop the backfill job for. */
            public StopBackfillJob setObject(java.lang.String object__) {
              if (!getSuppressPatternChecks()) {
                com.google.api.client.util.Preconditions.checkArgument(
                    OBJECT___PATTERN.matcher(object__).matches(),
                    "Parameter object__ must conform to the pattern "
                        + "^projects/[^/]+/locations/[^/]+/streams/[^/]+/objects/[^/]+$");
              }
              this.object__ = object__;
              return this;
            }

            @Override
            public StopBackfillJob set(String parameterName, Object value) {
              return (StopBackfillJob) super.set(parameterName, value);
            }
          }
        }
      }
    }
  }

  /**
   * Builder for {@link Datastream}.
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

    /** Builds a new instance of {@link Datastream}. */
    @Override
    public Datastream build() {
      return new Datastream(this);
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
     * Set the {@link DatastreamRequestInitializer}.
     *
     * @since 1.12
     */
    public Builder setDatastreamRequestInitializer(
        DatastreamRequestInitializer datastreamRequestInitializer) {
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

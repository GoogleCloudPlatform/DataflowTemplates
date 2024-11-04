package com.google.cloud.teleport.v2.templates.utils;

public interface ISourceDao {

    /**
     * Initializes the DAO with the necessary connection parameters.
     *
     * @param url      Connection URL.
     * @param user     Database user.
     * @param password User password.
     */
    void initialize(String url, String user, String password) throws Exception;

    /**
     * Executes a given write statement.
     *
     * @param statement The SQL or query statement.
     * @throws Exception If there is an error executing the statement.
     */
    void write(String statement) throws Exception;

    /**
     * Closes any open connections.
     */
    void close() throws Exception;
}


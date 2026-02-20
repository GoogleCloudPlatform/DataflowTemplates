package org.apache.beam.it.gcp.firestore;

/** Custom exception for {@link FirestoreResourceManager} operations. */
public class FirestoreResourceManagerException extends RuntimeException {

  public FirestoreResourceManagerException(String message) {
    super(message);
  }

  public FirestoreResourceManagerException(String message, Throwable cause) {
    super(message, cause);
  }
}
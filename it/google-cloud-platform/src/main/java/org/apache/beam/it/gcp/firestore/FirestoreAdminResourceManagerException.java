package org.apache.beam.it.gcp.firestore;

/** Custom exception for {@link FirestoreAdminResourceManagerException} operations. */
public class FirestoreAdminResourceManagerException extends RuntimeException {

  public FirestoreAdminResourceManagerException(String message) {
    super(message);
  }

  public FirestoreAdminResourceManagerException(String message, Throwable cause) {
    super(message, cause);
  }
}

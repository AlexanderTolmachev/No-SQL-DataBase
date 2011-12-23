package ru.compscicenter.db.tolmachev.nosqldb.server.exceptions;

public class DBUnavailabilityException extends Exception {

    public DBUnavailabilityException() {
    }

    public DBUnavailabilityException(String message) {
        super(message);
    }

    public DBUnavailabilityException(String message, Throwable cause) {
        super(message, cause);
    }

    public DBUnavailabilityException(Throwable cause) {
        super(cause);
    }
}

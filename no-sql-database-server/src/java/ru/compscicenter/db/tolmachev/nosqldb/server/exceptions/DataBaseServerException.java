package ru.compscicenter.db.tolmachev.nosqldb.server.exceptions;

public class DataBaseServerException extends Exception {
    public DataBaseServerException() {
    }

    public DataBaseServerException(String message) {
        super(message);
    }

    public DataBaseServerException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBaseServerException(Throwable cause) {
        super(cause);
    }
}

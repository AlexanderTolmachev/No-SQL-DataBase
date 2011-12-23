package ru.compscicenter.db.tolmachev.nosqldb.server.exceptions;

public class DataBaseRequestException extends Exception {
    public DataBaseRequestException() {
    }

    public DataBaseRequestException(String message) {
        super(message);
    }

    public DataBaseRequestException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBaseRequestException(Throwable cause) {
        super(cause);
    }
}
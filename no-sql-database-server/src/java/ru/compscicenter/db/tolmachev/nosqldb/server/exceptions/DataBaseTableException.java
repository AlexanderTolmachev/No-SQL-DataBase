package ru.compscicenter.db.tolmachev.nosqldb.server.exceptions;

public class DataBaseTableException extends Exception{

    public DataBaseTableException() {
    }

    public DataBaseTableException(String message) {
        super(message);
    }

    public DataBaseTableException(String message, Throwable cause) {
        super(message, cause);
    }

    public DataBaseTableException(Throwable cause) {
        super(cause);
    }
}

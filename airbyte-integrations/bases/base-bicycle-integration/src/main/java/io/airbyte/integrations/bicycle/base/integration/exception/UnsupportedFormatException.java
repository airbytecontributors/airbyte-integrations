package io.airbyte.integrations.bicycle.base.integration.exception;

public class UnsupportedFormatException extends Exception {

    private String fileName;

    public UnsupportedFormatException(String fileName) {
        this.fileName = fileName;
    }

    public UnsupportedFormatException(String fileName, String message) {
        super(message);
        this.fileName = fileName;
    }

    public UnsupportedFormatException(String fileName, String message, Throwable cause) {
        super(message, cause);
        this.fileName = fileName;
    }

    public UnsupportedFormatException(String fileName, Throwable cause) {
        super(cause);
        this.fileName = fileName;
    }

    public UnsupportedFormatException(String fileName, String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
        this.fileName = fileName;
    }
}

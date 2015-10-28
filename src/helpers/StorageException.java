package helpers;

/**
 * Created by aacha on 10/28/2015.
 */
public class StorageException extends Exception {
    private static final long serialVersionUID = 1L;
    private String errorMessage;

    public StorageException(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return this.errorMessage;
    }
}

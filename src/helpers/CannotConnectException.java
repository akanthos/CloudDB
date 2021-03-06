package helpers;

/**
 * Generic exception class used thoughout the program.
 */
public class CannotConnectException extends Exception {

	private static final long serialVersionUID = 1L;
	private String errorMessage;

	public CannotConnectException(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getErrorMessage() {
		return this.errorMessage;
	}
}

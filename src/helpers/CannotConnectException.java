package helpers;

public class CannotConnectException extends Exception {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
//	private String errorCode;
	private String errorMessage; 
	public CannotConnectException(String errorMessage) {
//		this.errorCode = errorCode;
		this.errorMessage = errorMessage;
	}
//	public String getErrorCode() {
//		return this.errorCode;
//	}
	public String getErrorMessage() {
		return this.errorMessage;
	}
}

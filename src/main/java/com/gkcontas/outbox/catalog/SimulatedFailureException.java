package com.gkcontas.outbox.catalog;

/**
 * The failure the demonstration endpoints raise on purpose.
 *
 * <p>A dedicated type rather than {@code IllegalStateException} so the error handler can
 * answer 500 for exactly this and nothing else — a handler that catches every
 * {@code IllegalStateException} would quietly turn real bugs into tidy responses.
 */
public class SimulatedFailureException extends RuntimeException {

    public SimulatedFailureException(String message) {
        super(message);
    }
}

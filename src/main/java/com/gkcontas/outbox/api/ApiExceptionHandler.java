package com.gkcontas.outbox.api;

import com.gkcontas.outbox.catalog.DuplicateSkuException;
import com.gkcontas.outbox.catalog.ProductNotFoundException;
import com.gkcontas.outbox.catalog.SimulatedFailureException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.validation.BindException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler(ProductNotFoundException.class)
    ProblemDetail handleNotFound(ProductNotFoundException exception) {
        return problem(HttpStatus.NOT_FOUND, "Product not found", exception.getMessage());
    }

    @ExceptionHandler(DuplicateSkuException.class)
    ProblemDetail handleDuplicate(DuplicateSkuException exception) {
        return problem(HttpStatus.CONFLICT, "Duplicate sku", exception.getMessage());
    }

    @ExceptionHandler(SimulatedFailureException.class)
    ProblemDetail handleSimulated(SimulatedFailureException exception) {
        return problem(HttpStatus.INTERNAL_SERVER_ERROR, "Simulated failure", exception.getMessage());
    }

    @ExceptionHandler(BindException.class)
    ProblemDetail handleValidation(BindException exception) {
        String detail = exception.getFieldErrors().stream()
                .map(error -> "%s %s".formatted(error.getField(), error.getDefaultMessage()))
                .sorted()
                .reduce((left, right) -> left + "; " + right)
                .orElse("The request is not valid.");
        return problem(HttpStatus.BAD_REQUEST, "Invalid request", detail);
    }

    private static ProblemDetail problem(HttpStatus status, String title, String detail) {
        ProblemDetail problemDetail = ProblemDetail.forStatus(status);
        problemDetail.setTitle(title);
        problemDetail.setDetail(detail);
        return problemDetail;
    }
}

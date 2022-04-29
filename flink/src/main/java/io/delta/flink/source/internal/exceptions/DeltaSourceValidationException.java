package io.delta.flink.source.internal.exceptions;

import java.util.Collection;
import java.util.Collections;

public class DeltaSourceValidationException extends RuntimeException {

    /**
     * Path to Delta table for which exception was thrown. Can be null if exception was thrown on
     * missing path to Delta table.
     */
    private final String tablePath;

    private final Collection<String> validationMessages;

    public DeltaSourceValidationException(String tablePath, String validationMessage) {
        this.tablePath = String.valueOf(tablePath);
        this.validationMessages = Collections.singletonList(validationMessage);
    }

    public DeltaSourceValidationException(String tablePath, Collection<String> validationMessages) {
        this.tablePath = String.valueOf(tablePath);
        this.validationMessages =
            (validationMessages == null) ? Collections.emptyList() : validationMessages;
    }

    @Override
    public String getMessage() {

        String validationMessages = String.join(System.lineSeparator(), this.validationMessages);

        return "Invalid Delta Source definition detected."
            + System.lineSeparator()
            + "The reported issues are:"
            + System.lineSeparator()
            + validationMessages;
    }

    public String getTablePath() {
        return tablePath;
    }

    public Collection<String> getValidationMessages() {
        return Collections.unmodifiableCollection(this.validationMessages);
    }
}

package io.delta.standalone.data;

import io.delta.standalone.actions.Action;
import io.delta.standalone.internal.util.ConversionUtils;

import java.io.IOException;

public class ActionCloseableIterator implements CloseableIterator<Action> {

    CloseableIterator<String> stringIterator;

    public ActionCloseableIterator(CloseableIterator<String> stringIterator) {
        this.stringIterator = stringIterator;
    }

    @Override
    public boolean hasNext() {
        return stringIterator.hasNext();
    }

    @Override
    public Action next() {
        return ConversionUtils.convertAction(
                io.delta.standalone.internal.actions.Action.fromJson(stringIterator.next()));
    }

    @Override
    public void close() throws IOException {
        stringIterator.close();
    }
}

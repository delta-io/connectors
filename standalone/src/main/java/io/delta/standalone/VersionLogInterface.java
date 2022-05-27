package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.internal.data.ActionCloseableIterator;

import java.util.List;

/**
 * {@link VersionLog} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */

public interface VersionLogInterface {

    public long getVersion();

    public List<Action> getActions();

    ActionCloseableIterator getActionIterator();
}

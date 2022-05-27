package io.delta.standalone;

import io.delta.standalone.actions.Action;
import io.delta.standalone.internal.data.ActionCloseableIterator;

import java.util.List;

/**
 * {@link VersionLog} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */

public interface VersionLogInterface {

    /**
     * @return the table version at which these actions occurred
     */
    long getVersion();

    /**
     * @return an unmodifiable {@code List} of the actions for this table version
     */
    List<Action> getActions();

    /**
     * @return an {@code Iterator} of the list of actions for this table version
     */
    ActionCloseableIterator getActionIterator();
}

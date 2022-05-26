/*
 * Copyright (2020-present) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.standalone;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import io.delta.standalone.data.CloseableIterator;
import javax.annotation.Nonnull;

import io.delta.standalone.actions.Action;

/**
 * {@link VersionLog} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */
public final class VersionLog {
    private final long version;

    private final List<Action> actions;

    final CloseableIterator<Action> actionsIter;

    @Deprecated
    public VersionLog(long version, @Nonnull List<Action> actions) {
        this.version = version;
        this.actions = actions;
        this.actionsIter = null; // TODO: make some sort of iter wrapper using this.actions + index
    }

    public VersionLog(long version, @Nonnull CloseableIterator<Action> actionsIter) {
        this.version = version;
        this.actions = null;
        this.actionsIter = actionsIter;
    }

    /**
     * @return the table version at which these actions occurred
     */
    public long getVersion() {
        return version;
    }

    public boolean hasNext() {
        if (actionsIter != null) {
            return actionsIter.hasNext();
        } else {
            // TODO use the iter wrapping from the List<Action> constructor
        }
    }

    public Action next() throws NoSuchElementException {
        if (actionsIter != null) {
            return actionsIter.next();
        } else {
            // TODO use the iter wrapping from the List<Action> constructor
        }
    }

    public void close() throws IOException {
        if (actionsIter != null) {
            actionsIter.close();
        }
    }

    /**
     * @return an unmodifiable {@code List} of the actions for this table version
     */
    @Deprecated
    @Nonnull
    public List<Action> getActions() {
        if (actionsIter != null) {
            // build list using actionsIter
        } else {
            return Collections.unmodifiableList(actions);
        }
    }
}

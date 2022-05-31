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
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;

import io.delta.standalone.actions.Action;
import io.delta.standalone.data.CloseableIterator;

/**
 * {@link VersionLog} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */
public class VersionLog {
    private final long version;

    private final List<Action> actions;

    private Iterator<Action> actionIterator;

    public VersionLog(long version, @Nonnull List<Action> actions) {
        this.version = version;
        this.actions = actions;
        this.actionIterator = actions.iterator();
    }

    /**
     * @return the table version at which these actions occurred
     */
    public long getVersion() {
        return version;
    }

    /**
     * @return an unmodifiable {@code List} of the actions for this table version
     */
    @Nonnull
    public List<Action> getActions() {
        return Collections.unmodifiableList(actions);
    }

    /**
     * @return an {@code CloseableIterator} of the actions for this table version
     */
    @Nonnull
    public CloseableIterator<Action> getActionIterator() {

        // reset the iterator
        actionIterator = actions.iterator();

        return new CloseableIterator<Action>() {
            @Override
            public void close() throws IOException {
            }

            @Override
            public boolean hasNext() {
                return actionIterator.hasNext();
            }

            @Override
            public Action next() {
                return actionIterator.next();
            }
        };
    }
}

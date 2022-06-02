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
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

import io.delta.storage.CloseableIterator;

import io.delta.standalone.actions.Action;
import io.delta.standalone.internal.util.ConversionUtils;

/**
 * {@link VersionLog} is the representation of all actions (changes) to the Delta Table
 * at a specific table version.
 */
public final class VersionLog {
    private final long version;

    private List<Action> actions;

    private final Supplier<CloseableIterator<String>> actionsSupplier;

    public VersionLog(long version, @Nonnull Supplier<CloseableIterator<String>> actionsSupplier) {
        this.version = version;
        this.actionsSupplier = actionsSupplier;
        this.actions = null;
    }

    public VersionLog(long version, @Nonnull List<Action> actions) {
        this.version = version;
        this.actionsSupplier = null;
        this.actions = actions;
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
        synchronized(this) {
            if (actions == null) {
                List<Action> localActions = new ArrayList<>();
                try(CloseableIterator<Action> actionIterator = getActionsIterator()) {
                    while (actionIterator.hasNext()) {
                        localActions.add(actionIterator.next());
                    }
                    actions = localActions;
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return Collections.unmodifiableList(actions);
    }

    /**
     * @return an {@code CloseableIterator} of the actions for this table version
     */
    @Nonnull
    public CloseableIterator<Action> getActionsIterator() {
        synchronized (this) {
            if (actionsSupplier != null) {
                // create a `CloseableIterator<Action>` from `CloseableIterator<String>`
                return new CloseableIterator<Action>() {
                    private final CloseableIterator<String> stringIterator = actionsSupplier.get();

                    @Override
                    public boolean hasNext() {
                        return stringIterator.hasNext();
                    }

                    @Override
                    public Action next() {
                        return ConversionUtils.convertAction(
                                io.delta.standalone.internal.actions.Action.fromJson(
                                        stringIterator.next()));
                    }

                    @Override
                    public void close() throws IOException {
                        stringIterator.close();
                    }
                };
            } else {
                // create a `CloseableIterator` from `actions`
                return new CloseableIterator<Action>() {
                    final Iterator<Action> actionIterator = actions.iterator();
                    @Override
                    public void close() {}

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
    }
}

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

import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import io.delta.standalone.actions.Action;
import io.delta.standalone.internal.data.ActionCloseableIterator;

public class VersionLog implements VersionLogInterface {
    private final long version;

    @Nonnull
    private final List<Action> actions;

    public VersionLog(long version, @Nonnull List<Action> actions) {
        this.version = version;
        this.actions = actions;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Nonnull @Override
    public List<Action> getActions() {
        return Collections.unmodifiableList(actions);
    }

    @Override
    public ActionCloseableIterator getActionIterator() {
        return null; // TODO: find suitable returning
    }
}

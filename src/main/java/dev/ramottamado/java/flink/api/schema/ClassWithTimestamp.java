/*
 * Copyright 2021 Tamado Sitohang <ramot@ramottamado.dev>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.ramottamado.java.flink.api.schema;

import java.io.Serializable;

import dev.ramottamado.java.flink.annotation.Internal;

/**
 * {@code ClassWithTimestamp} provides base class for classes that implements timestamp for use in Flink
 * {@code ProcessFunction} {@code onTimer} methods.
 *
 * @author Tamado Sitohang
 * @since  1.0
 */
@Internal
public abstract class ClassWithTimestamp implements Serializable {
    private static final long serialVersionUID = 1L;
    private long timestamp;

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }
}

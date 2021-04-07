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

package dev.ramottamado.java.flink.functions.helper;

import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

public class TestSourceFunction<T> implements ParallelSourceFunction<T>, ResultTypeQueryable<T> {
    private static final long serialVersionUID = -7782697375127418568L;
    private volatile boolean isRunning = true;
    private final Class<T> type;
    private List<T> sources;

    public TestSourceFunction(List<T> sources, Class<T> type) {
        this.sources = sources;
        this.type = type;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        for (T source : sources) {
            if (!isRunning)
                return;
            ctx.collect(source);
        }

        Thread.sleep(1000);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(type);
    }
}

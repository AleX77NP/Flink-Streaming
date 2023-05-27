/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package galiglobal.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Locale;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="https://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

    private SourceFunction<Pokemon> source;
    private SinkFunction<Pokemon> sink;

    public StreamingJob(SourceFunction<Pokemon> source, SinkFunction<Pokemon> sink) {
        this.source = source;
        this.sink = sink;
    }

    public void execute() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Pokemon> PokemonDataStream =
                env.addSource(source)
                        .returns(TypeInformation.of(Pokemon.class));

        PokemonDataStream
                .map(new ModifyName())
                .addSink(sink);

        env.execute();
    }

    public class ModifyName implements MapFunction<Pokemon, Pokemon> {

        @Override
        public Pokemon map(Pokemon pokemon) throws Exception {
            if (pokemon.getHeight() > 12)
                pokemon.setName(pokemon.getName().toUpperCase(Locale.ROOT));
            else
                pokemon.setName(pokemon.getName().toLowerCase(Locale.ROOT));
            return pokemon;
        }
    }

    public static void main(String[] args) throws Exception {
        StreamingJob streamingJob = new StreamingJob(new PokemonStreamSource(), new PrintSinkFunction<>());
        streamingJob.execute();
    }
}

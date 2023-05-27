package galiglobal.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Random;

public class PokemonStreamSource extends RichParallelSourceFunction<Pokemon> {

    private volatile boolean cancelled = false;
    private PokemonFetcher pokemonFetcher;
    private Random random;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        pokemonFetcher = new PokemonFetcher();
        random = new Random();
    }

    private String generateRandomId() {
        int randomId = 1 + random.nextInt(500);
        return String.valueOf(randomId);
    }

    @Override
    public void run(SourceContext<Pokemon> sourceContext) throws Exception {
        while (!cancelled) {
            Pokemon newPokemon = pokemonFetcher.fetchPokemon(generateRandomId());
            synchronized (sourceContext.getCheckpointLock()) {
                sourceContext.collect(newPokemon);
            }
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}

package galiglobal.flink;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

public class PokemonFetcher {

    private String apiUrl;
    private HttpClient httpClient;

    public PokemonFetcher() {
        this.apiUrl = "https://pokeapi.co/api/v2/pokemon/";
        httpClient = HttpClient.newHttpClient();
    }

    public Pokemon fetchPokemon(String pokemonId) {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl + pokemonId))
                .build();
        try {
            Pokemon randomPokemon = httpClient.sendAsync(req, HttpResponse.BodyHandlers.ofString())
                    .thenApply(HttpResponse::body)
                    .thenApply(PokemonFetcher::parse)
                    .join();
            return randomPokemon;
        } catch (Exception e) {
            System.out.println("ERROR!");
            System.out.println(e);
            return new Pokemon("UNKNOWN_2", -2, -2, -2, false);
        }
    }

    public static Pokemon parse(String responseBody) {
        JSONObject pokemonData = new JSONObject(responseBody);

        try {
            String name = pokemonData.getString("name");
            int height = pokemonData.getInt("height");
            int weight = pokemonData.getInt("weight");
            int baseExperience = pokemonData.getInt("base_experience");
            boolean isDefault = pokemonData.getBoolean("is_default");

            return new Pokemon(name, height, weight, baseExperience, isDefault);
        } catch (Exception e) {
            System.out.println(e);
            return new Pokemon("UNKNOWN_1", -1, -1, -1, false);
        }
    }


}

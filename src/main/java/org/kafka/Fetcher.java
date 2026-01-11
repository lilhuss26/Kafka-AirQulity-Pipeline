package org.kafka;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import io.github.cdimascio.dotenv.Dotenv;

public class Fetcher {
    public String fetchData (int id) throws Exception {
        Dotenv dotenv = Dotenv.load();
        String apiKey = dotenv.get("API_KEY");

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(new URI(String.format("https://api.openaq.org/v3/locations/%d/latest", id)))
                .header("X-API-Key", apiKey)
                .GET()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

        return response.body();
    }
}

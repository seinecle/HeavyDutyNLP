/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.clementlevallois.heavydutynlp.api;

/**
 *
 * @author LEVALLOIS
 */
import io.javalin.Javalin;
import java.io.StringReader;
import java.util.Iterator;
import java.util.TreeMap;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import net.clementlevallois.heavydutynlp.LemmatizerSpark;

public class ApiController {

    public static void main(String[] args) {
        Javalin app = Javalin.create().start(7000);
        System.out.println("running the api");
        LemmatizerSpark.initializeSpark();

        app.post("/lemmatize/{lang}", ctx -> {
            TreeMap<Integer, String> lines = new TreeMap();
            String body = ctx.body();
            if (body.isEmpty()) {
                ctx.status(500);
            } else {
                JsonReader jsonReader = Json.createReader(new StringReader(body));
                JsonObject jsonObject = jsonReader.readObject();
                Iterator<String> iteratorKeys = jsonObject.keySet().iterator();
                int i = 0;
                while (iteratorKeys.hasNext()) {
                    String line = jsonObject.getString(iteratorKeys.next());
                    lines.put(i++, line);
                }
                LemmatizerSpark lemmatizer = new LemmatizerSpark();
                TreeMap<Integer, String> lemmatized = lemmatizer.lemmatize(lines, ctx.pathParam("lang"));
                ctx.json(lemmatized);
            }
        });
    }
}

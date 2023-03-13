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
import io.javalin.http.HttpStatus;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import net.clementlevallois.heavydutynlp.lemmatizing.LemmatizerSpark;
import net.clementlevallois.umigon.model.NGram;
import net.clementlevallois.utils.Multiset;

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

        app.post("/lemmatize/multiset/{lang}", ctx -> {
            TreeMap<String, Integer> entries = new TreeMap();
            byte[] bodyAsBytes = ctx.bodyAsBytes();
            String body = new String(bodyAsBytes, StandardCharsets.UTF_8);
            if (body.isEmpty()) {
                ctx.status(500);
            } else {
                JsonReader jsonReader = Json.createReader(new StringReader(body));
                JsonObject jsonObject = jsonReader.readObject();
                JsonObject linesAsJsonObject = jsonObject.getJsonObject("lines");
                Iterator<String> iteratorKeys = linesAsJsonObject.keySet().iterator();
                int i = 0;
                while (iteratorKeys.hasNext()) {
                    String nextTerm = iteratorKeys.next();
                    int countNextTerm = jsonObject.getInt(nextTerm);
                    entries.put(nextTerm, countNextTerm);
                }
                LemmatizerSpark lemmatizer = new LemmatizerSpark();
                TreeMap<String, Integer> lemmatized = lemmatizer.lemmatizeFromMultiset(entries, ctx.pathParam("lang"));
                ctx.json(lemmatized).status(HttpStatus.OK);
            }
        });
        app.post("/lemmatize/map/{lang}", ctx -> {
            byte[] bodyAsBytes = ctx.bodyAsBytes();

            if (bodyAsBytes.length == 0) {
                ctx.status(HttpURLConnection.HTTP_BAD_REQUEST);
            } else {
                ObjectInputStream ois = null;
                try {
                    ByteArrayInputStream bis = new ByteArrayInputStream(bodyAsBytes);
                    ois = new ObjectInputStream(bis);
                    TreeMap<Integer, String> mapInput = (TreeMap<Integer, String>) ois.readObject();
                    LemmatizerSpark lemmatizer = new LemmatizerSpark();
                    Map<Integer, String> lemmatizedFromMap = lemmatizer.lemmatizeFromMap(mapInput, ctx.pathParam("lang"));
                    ByteArrayOutputStream bos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(bos);
                    oos.writeObject(lemmatizedFromMap);
                    oos.flush();
                    byte[] data = bos.toByteArray();

                    ctx.result(data).status(HttpStatus.OK);
                } catch (IOException ex) {
                    Logger.getLogger(ApiController.class.getName()).log(Level.SEVERE, null, ex);
                } finally {
                    try {
                        ois.close();
                    } catch (IOException ex) {
                        Logger.getLogger(ApiController.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        });
    }
}

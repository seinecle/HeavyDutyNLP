/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package net.clementlevallois.heavydutynlp.lemmatizing;

/**
 *
 * @author LEVALLOIS
 */
import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.Finisher;
import com.johnsnowlabs.nlp.annotators.LemmatizerModel;
import com.johnsnowlabs.nlp.annotators.Tokenizer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.clementlevallois.heavydutynlp.config.Params;
import net.clementlevallois.umigon.model.NGram;
import net.clementlevallois.utils.Clock;
import net.clementlevallois.utils.Multiset;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import scala.collection.Iterator;
import scala.collection.mutable.WrappedArray;

public class LemmatizerSpark {

    private static SparkSession spark;
    private static String cacheFolder;
    private static String pathOnDisk;
    private String lang;

    public static void initializeSpark() {
        Clock clock = new Clock("launching a spark session");
        String osName = System.getProperty("os.name");
        if (osName.toLowerCase().contains("win")) {
            try {
                System.setProperty("hadoop.home.dir", Params.getHadoopHomeDir("win"));
                cacheFolder = Params.getCacheFolder("win");
                pathOnDisk = Params.getPathOnDisk("win");
            } catch (IOException ex) {
                Logger.getLogger(LemmatizerSpark.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            try {
                System.setProperty("hadoop.home.dir", Params.getHadoopHomeDir("lin"));
                cacheFolder = Params.getCacheFolder("lin");
                pathOnDisk = Params.getPathOnDisk("lin");
            } catch (IOException ex) {
                Logger.getLogger(LemmatizerSpark.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        spark = SparkSession.builder()
                .appName("Simple Application")
                .config("spark.master", "local")
                .config("spark.jsl.settings.pretrained.cache_folder", cacheFolder)
                .getOrCreate();
        clock.closeAndPrintClock();

    }

    public TreeMap<Integer, String> lemmatize(TreeMap<Integer, String> inputs, String lang) throws IOException {
        if (inputs == null || lang == null) {
            return inputs;
        }
        List<String> lines = new ArrayList();
        for (Map.Entry<Integer, String> entry : inputs.entrySet()) {
            lines.add(entry.getValue());
        }
        List<String> lemmatized = lemmatize(lines, lang);
        int i = 0;
        for (String line : lemmatized) {
            inputs.put(i++, line);
        }
        return inputs;
    }

    public TreeMap<String, Integer> lemmatizeFromMultiset(TreeMap<String, Integer> inputs, String lang) throws IOException {
        if (inputs == null || lang == null) {
            return inputs;
        }
        List<String> originalTerms = new ArrayList();
        for (Map.Entry<String, Integer> entry : inputs.entrySet()) {
            originalTerms.add(entry.getKey());
        }
        List<String> lemmatizedTerms = lemmatize(originalTerms, lang);

        TreeMap<String, Integer> results = new TreeMap();

        int i = 0;
        for (String termLemmatized : lemmatizedTerms) {
            int indexOf = lemmatizedTerms.indexOf(termLemmatized);
            String originalTerm = originalTerms.get(indexOf);
            Integer freq = inputs.get(originalTerm);
            results.put(termLemmatized, freq);
        }
        return results;
    }

    public Multiset<NGram> lemmatizeFromMultisetOfNGrams(Multiset<NGram> inputs, String lang) throws IOException {
        if (inputs == null || lang == null) {
            return inputs;
        }

        Multiset<NGram> results = new Multiset();
        List<NGram> originalNgrams = new ArrayList();
        for (NGram ngram : inputs.getElementSet()) {
            originalNgrams.add(ngram);
        }
        List<String> originalTerms = new ArrayList();
        for (NGram ngram : originalNgrams) {
            originalTerms.add(ngram.getOriginalForm());
        }
        List<String> lemmatizedTerms = lemmatize(originalTerms, lang);

        for (String termLemmatized : lemmatizedTerms) {
            int indexOf = lemmatizedTerms.indexOf(termLemmatized);
            NGram originalNgram = originalNgrams.get(indexOf);
            originalNgram.setOriginalFormLemmatized(termLemmatized);
            results.addSeveral(originalNgram, inputs.getCount(originalNgram));
        }
        return results;
    }

    public Map<Integer, String> lemmatizeFromMap(TreeMap<Integer, String> inputs, String lang) throws IOException {
        if (inputs == null || lang == null) {
            return inputs;
        }

        Map<Integer, String> results = new HashMap();
        List<String> originalTerms = new ArrayList();
        for (Map.Entry<Integer, String> entry : inputs.entrySet()) {
            originalTerms.add(entry.getValue());
        }
        List<String> lemmatizedTerms = lemmatize(originalTerms, lang);

        int i = 0;
        for (Map.Entry<Integer, String> entry : inputs.entrySet()) {
            results.put(entry.getKey(),lemmatizedTerms.get(i++));
        }
        return results;
    }

    public List<String> lemmatize(List<String> inputs, String langParam) throws IOException {
        if (inputs == null || langParam == null) {
            return inputs;
        }
//        Clock totalClock = new Clock("total clock");
//
//        Clock clock = new Clock("creation of the pipe");
        DocumentAssembler document = new DocumentAssembler();
        document.setInputCol("text");
        document.setOutputCol("document");
        document.setCleanupMode("disabled");

        Tokenizer tokenizer = new Tokenizer();
        tokenizer.setInputCols(new String[]{"document"});
        tokenizer.setOutputCol("token");
        String modelName;

        if (langParam.equals("en")) {
            modelName = "lemma_antbnc";
        } else {
            modelName = "lemma";
        }
        if (langParam.equals("no")) {
            lang = "nb";
        } else {
            lang = langParam;
        }

        LemmatizerModel lemmatizer;
        String pathOfModelOnDiskAsString = pathOnDisk + File.separator + lang;
        Path modelOnDisk = Path.of(pathOfModelOnDiskAsString);
        if (!Files.exists(modelOnDisk)) {
            System.out.println("downloading new lemma model for " + lang);
            lemmatizer = (LemmatizerModel) LemmatizerModel.pretrained(modelName, lang);
            lemmatizer.setInputCols(new String[]{"token"});
            lemmatizer.setOutputCol("lemma");
            File f = new File(pathOnDisk);
            String[] list = f.list((dir, name) -> name.startsWith("lemma_" + lang));
            if (list.length > 0) {
                copyDirectory(pathOnDisk + File.separator + list[0], pathOfModelOnDiskAsString);
                deleteDirectory(Path.of(pathOnDisk + File.separator + list[0]).toFile());
            }

        } else {
            System.out.println("lemma model for " + lang + " found on disk, loading it directly!");
            lemmatizer = (LemmatizerModel) LemmatizerModel.load(cacheFolder + File.separator + lang);
        }
        Finisher finisher = new Finisher();
        finisher.setInputCols(new String[]{"lemma"});

        Pipeline pipeline = new Pipeline();
        pipeline.setStages(new PipelineStage[]{document, tokenizer, lemmatizer, finisher});
//        clock.closeAndPrintClock();
//
//        clock = new Clock("creating the dataset from the text");
        Dataset<Row> data = spark.createDataset(inputs, Encoders.STRING()).toDF("text");
//        clock.closeAndPrintClock();

//        clock = new Clock("fitting the data");
        PipelineModel pipelineModel = pipeline.fit(data);
//        clock.closeAndPrintClock();
//
//        clock = new Clock("transforming the data");
        Dataset<Row> transformed = pipelineModel.transform(data);
//        clock.closeAndPrintClock();
//
//        clock = new Clock("exporting the results");
        transformed.selectExpr("explode(finished_lemma)");
        List<Row> rows = transformed.selectExpr("finished_lemma").collectAsList();
        List<String> results = new ArrayList();
        StringBuilder sb;
        for (Row row : rows) {
            sb = new StringBuilder();
            for (int i = 0; i < row.length(); i++) {
                WrappedArray array = (WrappedArray) row.get(i);
                Iterator iterator = array.iterator();
                while (iterator.hasNext()) {
                    Object next = iterator.next();
                    sb.append(next).append(" ");
                }
            }
            results.add(sb.toString().trim());
        }
//        clock.closeAndPrintClock();
//        totalClock.closeAndPrintClock();

        return results;

    }

    public static void main(String[] args) throws IOException {

        List<String> text;

        text = new ArrayList();
        text.add("student");
        text.add("studenter");
        text.add("studentene");

        LemmatizerSpark.initializeSpark();
        List<String> lemmatized = new LemmatizerSpark().lemmatize(text, "nb");

        System.out.println("input: " + text.toString());
        System.out.println("output: " + lemmatized.toString());
    }

    public static void copyDirectory(String sourceDirectoryLocation, String destinationDirectoryLocation)
            throws IOException {
        Files.walk(Paths.get(sourceDirectoryLocation))
                .forEach(source -> {
                    Path destination = Paths.get(destinationDirectoryLocation, source.toString()
                            .substring(sourceDirectoryLocation.length()));
                    try {
                        Files.copy(source, destination);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
    }

    static boolean deleteDirectory(File directoryToBeDeleted) {
        File[] allContents = directoryToBeDeleted.listFiles();
        if (allContents != null) {
            for (File file : allContents) {
                deleteDirectory(file);
            }
        }
        return directoryToBeDeleted.delete();
    }

}

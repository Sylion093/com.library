package com.library;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.IOException;
import java.io.InputStream;
import java.text.Normalizer;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PDFSearchMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Set<String> conectores;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String conectoresStr = conf.get("conectores",
                "el,la,los,las,un,una,unos,unas,y,o,en,de,del,por,para,con,sin,a,ante,bajo,cabe,contra,desde,durante,entre,hacia,hasta,mediante,según,sobre,tras,mi,tu,su,me,te,se,nos,os,que,qué,quien,quién,cual,cuál,donde,dónde,cuando,cuándo,como,cómo");

        conectores = new HashSet<>(Arrays.asList(conectoresStr.split(",")));
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String filePath = value.toString();
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        // Obtener el texto de búsqueda
        String searchText = conf.get("searchText");
        if (searchText == null || searchText.isEmpty()) {
            System.err.println("Texto de búsqueda no configurado.");
            return;
        }

        // Normalizar texto de búsqueda
        searchText = normalizarTexto(searchText.toLowerCase());
        Set<String> searchWords = new HashSet<>(Arrays.asList(searchText.split("\\s+")));
        searchWords.removeAll(conectores);

        try (InputStream is = fs.open(new Path(filePath));
             PDDocument document = PDDocument.load(is)) {

            PDFTextStripper pdfStripper = new PDFTextStripper();
            String text = pdfStripper.getText(document).toLowerCase();
            text = normalizarTexto(text);

            int maxMatches = 0;
            String bestSnippet = "";

            for (String word : searchWords) {
                Pattern pattern = Pattern.compile("(.{0,50}" + Pattern.quote(word) + ".{0,50})");
                Matcher matcher = pattern.matcher(text);

                while (matcher.find()) {
                    String snippet = matcher.group(1);
                    int count = 0;
                    for (String searchWord : searchWords) {
                        if (snippet.contains(searchWord)) count++;
                    }

                    if (count > maxMatches) {
                        maxMatches = count;
                        bestSnippet = snippet;
                    }
                }
            }

            if (maxMatches > 0) {
                context.write(new Text(new Path(filePath).getName()), new Text(bestSnippet + " [" + maxMatches + " matches]"));
            }
        } catch (Exception e) {
            System.err.println("Error procesando archivo: " + filePath + " - " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Normaliza el texto eliminando acentos y caracteres especiales.
     */
    private String normalizarTexto(String texto) {
        return Normalizer.normalize(texto, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "");
    }
}

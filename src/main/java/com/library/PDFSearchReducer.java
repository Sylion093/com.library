package com.library;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.PriorityQueue;

public class PDFSearchReducer extends Reducer<Text, Text, Text, Text> {

    private static class BookMatch {
        String bookTitle;
        String snippet;
        int matches;

        BookMatch(String bookTitle, String snippet, int matches) {
            this.bookTitle = bookTitle;
            this.snippet = snippet;
            this.matches = matches;
        }
    }

    private PriorityQueue<BookMatch> topMatches = new PriorityQueue<>(3, (m1, m2) -> {
        if (m1.matches != m2.matches) {
            return Integer.compare(m1.matches, m2.matches);
        }
        return m1.bookTitle.compareTo(m2.bookTitle);
    });

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            String snippet = value.toString();
            if (!snippet.contains(" [") || !snippet.endsWith(" matches]")) {
                System.err.println("Formato inválido: " + snippet);
                continue;
            }

            try {
                String[] parts = snippet.split(" \\[");
                int matches = Integer.parseInt(parts[1].replace(" matches]", ""));

                // Evita duplicados
                if (topMatches.stream().anyMatch(match -> match.snippet.equals(parts[0]))) {
                    continue;
                }

                BookMatch match = new BookMatch(key.toString(), parts[0], matches);
                topMatches.add(match);
                if (topMatches.size() > 3) {
                    topMatches.poll();
                }
            } catch (Exception e) {
                System.err.println("Error procesando el valor: " + snippet);
                e.printStackTrace();
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        while (!topMatches.isEmpty()) {
            BookMatch match = topMatches.poll();
            context.write(new Text(match.bookTitle), new Text(match.snippet + " [" + match.matches + " matches]"));
        }
    }
}

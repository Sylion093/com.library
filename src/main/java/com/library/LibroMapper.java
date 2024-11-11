package com.library;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.text.PDFTextStripper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class LibroMapper extends Mapper<Text, BytesWritable, Text, Text> {
    private final Text tema = new Text();
    private final Text libro = new Text();
    private final Map<String, String[]> diccionarios = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);
        Path diccionarioDir = new Path("/diccionarios/");

        for (FileStatus status : fs.listStatus(diccionarioDir)) {
            String nombreTema = status.getPath().getName().replace(".txt", "");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(status.getPath())));
            StringBuilder sb = new StringBuilder();
            String linea;
            while ((linea = reader.readLine()) != null) {
                sb.append(linea).append(" ");
            }
            diccionarios.put(nombreTema, sb.toString().split("\\s+"));
        }
    }

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        String tituloLibro = key.toString();
        libro.set(tituloLibro);

        PDDocument pdfDocument = PDDocument.load(value.getBytes());
        String contenido = new PDFTextStripper().getText(pdfDocument);
        pdfDocument.close();

        Map<String, Integer> temaCoincidencias = new HashMap<>();
        for (Map.Entry<String, String[]> entry : diccionarios.entrySet()) {
            String temaActual = entry.getKey();
            String[] terminos = entry.getValue();
            int coincidencias = 0;
            for (String termino : terminos) {
                if (contenido.contains(termino)) {
                    coincidencias++;
                }
            }
            temaCoincidencias.put(temaActual, coincidencias);
        }

        String mejorTema = "";
        int maxCoincidencias = 0;
        for (Map.Entry<String, Integer> entry : temaCoincidencias.entrySet()) {
            if (entry.getValue() > maxCoincidencias) {
                maxCoincidencias = entry.getValue();
                mejorTema = entry.getKey();
            }
        }

        tema.set(mejorTema);
        context.write(tema, libro);
    }
}

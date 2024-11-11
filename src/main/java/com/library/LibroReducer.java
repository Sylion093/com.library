package com.library;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class LibroReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text tema, Iterable<Text> libros, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        FileSystem fs = FileSystem.get(conf);

        Path directorioTema = new Path("/biblioteca/" + tema.toString());
        if (!fs.exists(directorioTema)) {
            fs.mkdirs(directorioTema);
        }

        for (Text libro : libros) {
            Path destino = new Path(directorioTema, libro.toString());
            Path rutaOriginal = new Path("/libros/" + libro.toString());
            if (fs.exists(rutaOriginal)) {
                fs.rename(rutaOriginal, destino);
            }
            context.write(new Text("Clasificado en: " + tema), libro);
        }
    }
}

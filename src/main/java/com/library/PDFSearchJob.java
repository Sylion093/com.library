package com.library;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PDFSearchJob {

    public static boolean runJob(String[] args) {
        if (args.length != 3) {
            System.err.println("Uso: PDFSearchJob <ruta de entrada> <ruta de salida> <texto de búsqueda>");
            return false;
        }

        try {
            Configuration conf = new Configuration();
            conf.set("searchText", args[2]);

            Path inputPath = new Path(args[0]);
            Path outputPath = new Path(args[1]);

            // Validar rutas
            if (!inputPath.getFileSystem(conf).exists(inputPath)) {
                System.err.println("Error: La ruta de entrada no existe: " + args[0]);
                return false;
            }

            if (outputPath.getFileSystem(conf).exists(outputPath)) {
                System.err.println("Error: La ruta de salida ya existe: " + args[1]);
                return false;
            }

            // Configuración del trabajo
            Job job = Job.getInstance(conf, "PDF Search Job: " + args[2]);
            job.setJarByClass(PDFSearchJob.class);

            job.setMapperClass(PDFSearchMapper.class);
            job.setReducerClass(PDFSearchReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileInputFormat.addInputPath(job, inputPath);
            FileOutputFormat.setOutputPath(job, outputPath);

            // Configuración adicional (opcional)
            job.setNumReduceTasks(1); // Cambia según tus necesidades

            // Ejecutar el trabajo
            return job.waitForCompletion(true);

        } catch (Exception e) {
            System.err.println("Error durante la ejecución del trabajo: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }
}

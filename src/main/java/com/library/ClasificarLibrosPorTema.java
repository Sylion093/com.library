package com.library;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ClasificarLibrosPorTema {
    public static boolean runJob(String[] args) throws Exception {
    	try {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Clasificar Libros por Tema");

        job.setJarByClass(ClasificarLibrosPorTema.class);
        job.setMapperClass(LibroMapper.class);
        job.setReducerClass(LibroReducer.class);

        job.setInputFormatClass(WholeFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("/libros"));
        FileOutputFormat.setOutputPath(job, new Path("/biblioteca_temp"));

        return job.waitForCompletion(true);

        } catch (Exception e) {
            System.err.println("Error durante la ejecución del trabajo: " + e.getMessage());
            e.printStackTrace();
            return false;
        }
    }

}

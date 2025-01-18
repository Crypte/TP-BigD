package org.epf.hadoop.colfil1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob1 {
    public static void main(String[] args) throws Exception {
        System.out.println("Arguments received:");
        for (String arg : args) {
            System.out.println(arg);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Collaborative Filtering Job 1");

        job.setJarByClass(ColFilJob1.class);

        job.setMapperClass(RelationshipMapper.class);
        job.setReducerClass(RelationshipReducer.class);
        job.setInputFormatClass(RelationshipInputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        FileInputFormat.addInputPath(job, new Path(args[1])); // Chemin des données d'entrée
        FileOutputFormat.setOutputPath(job, new Path(args[2])); // Chemin des résultats
        System.out.println("Input Path: " + args[1]);
        System.out.println("Output Path: " + args[2]);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
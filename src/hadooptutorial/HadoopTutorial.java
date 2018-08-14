/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadooptutorial;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import javax.lang.model.SourceVersion;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.util.ToolRunner;

/**
 *
 * @author mehulkothari
 */
public class HadoopTutorial extends Configured implements Tool {         

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws Exception {
        
        int exitcode=ToolRunner.run(new HadoopTutorial(), args);
        System.exit(exitcode);
        // TODO code application logic here
    }

    public int run(String[] args) throws Exception {
         // Configuration processed by ToolRunner
         //Configuration conf = getConf();
         
         // Create a JobConf using the processed conf
         JobConf conf = new JobConf(HadoopTutorial.class);
         conf.setJobName("Word count");
         FileInputFormat.setInputPaths(conf, new Path(args[0]));
         FileOutputFormat.setOutputPath(conf, new Path(args[1]));
         
         
         // Process custom command-line options
         //Path in = new Path(args[1]);
         //Path out = new Path(args[2]);
         
         // Specify various job-specific parameters     
         //job.setJobName("my-app");
         //job.setInputPath(in);
         //job.setOutputPath(out);
         conf.setMapperClass(WordMapper.class);
         conf.setReducerClass(WordReducer.class);
         conf.setMapOutputKeyClass(Text.class);
         conf.setMapOutputValueClass(IntWritable.class);
         conf.setOutputKeyClass(IntWritable.class);
         conf.setOutputValueClass(Text.class);
         

         // Submit the job, then poll for progress until the job is complete
         JobClient.runJob(conf);
         return 0;
       }

   
    
}

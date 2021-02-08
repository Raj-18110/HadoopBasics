import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
//        String s = "1 janani jitu";
//        System.out.println(s.split(" ")[2]);
        int exitCode = ToolRunner.run(new Main(),args);
        System.exit(exitCode);

    }

    @Override
    public int run(String[] strings) throws Exception{
        Configuration configuration = this.getConf();

        // Added comment just to make a change in the file
        Job job = Job.getInstance(configuration);
        job.setJobName("ViewCount");
        job.setJarByClass(Main.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        Path inputFilePath = new Path(strings[0]);
        Path outputFilePath = new Path(strings[1]);

        FileInputFormat.addInputPath(job,inputFilePath);
        FileOutputFormat.setOutputPath(job,outputFilePath);


        return job.waitForCompletion(true) ? 0 : 1;
    }
}

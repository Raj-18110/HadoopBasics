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

        //Call the static run method of the ToolRunner class which in-turn calls the run method that we
        //have overrriden below of Tool interface
        int exitCode = ToolRunner.run(new Main(),args);
        System.exit(exitCode);

    }

    @Override
    public int run(String[] strings) throws Exception{
        //Creating configuration object from the method of Configured class that we extended called getConf
        Configuration configuration = this.getConf();

        //Creating Job Instance/ Object from Job's static method getInstance with Configuration object
        Job job = Job.getInstance(configuration);

        //Name of the Job
        job.setJobName("ViewCount");

        //Main class where Job code is declared and Configured is extended and Tool is impplemented
        job.setJarByClass(Main.class);

        //This creates the partitions based on the number mentioned.
        //By default it is 1 if not declared
        //If the partition code is not mentioned then hash function executes as a partition function by default
        job.setNumReduceTasks(2);

        //Mention the type of output key class type that should be resulted
        job.setOutputKeyClass(Text.class);

        //Mention the type of output value class type that should be resulted
        job.setOutputValueClass(IntWritable.class);


        //Pass the class where Mapper logic is implemented
        job.setMapperClass(Map.class);
        //Pass the class where Reducer logic is implemented
        job.setReducerClass(Reduce.class);

        //Pass the class where Reducer logic is implemented
        //Combiner is extension to the mapper and reduces the burden on reducer
        //by extending the map it utilizes parallelism
        job.setCombinerClass(Reduce.class);


        //Path is the type of class present in Hadoop

        Path inputFilePath = new Path(strings[0]);
        Path outputFilePath = new Path(strings[1]);

        //Pass the paths and job objects
        FileInputFormat.addInputPath(job,inputFilePath);
        FileOutputFormat.setOutputPath(job,outputFilePath);


        //This method starts and executes the job
        return job.waitForCompletion(true) ? 0 : 1;
    }
}

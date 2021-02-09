import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text,Text, IntWritable>{

    //Here we need to override the map method of the Mapper class and provide custom mapper logic here

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name =  value.toString().split(" ")[2];
        System.out.println("Job Name "+context.getJobName()+" Job ID "+context.getJobID());
        //Context is intimating mapReduce framework about the output of mapper
        //This is like a communication to the mapreduce framework
        //It is a bridge between our code and mapreduce framework
        context.write(new Text(name),new IntWritable(1));
    }
}

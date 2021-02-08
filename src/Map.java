import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class Map extends Mapper<LongWritable, Text,Text, IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String name =  value.toString().split(" ")[2];
        System.out.println("Job Name "+context.getJobName()+" Job ID "+context.getJobID());
        context.write(new Text(name),new IntWritable(1));
    }
}

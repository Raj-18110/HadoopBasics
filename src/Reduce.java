import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Reduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    //Here we need to override the reduce method of the Reducer class and provide custom reducer logic here
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int resultantCount = 0;
        for(IntWritable value : values)
            resultantCount += value.get();

        //Context is intimating mapReduce framework about the output of mapper
        //This is like a communication to the mapreduce framework
        //It is a bridge between our code and mapreduce framework
        context.write(key,new IntWritable(resultantCount));
    }
}

import java.io.IOException;
import java.util.*;
import java.util.Arrays;
import java.util.ArrayList;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CardCount {

public static class CardMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
  
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
    {	
        String allText = value.toString();
        //Scanner scanner = new Scanner(allText);
        String[] splits = allText.split(" ");
        IntWritable rank = new IntWritable(Integer.parseInt(splits[1]));
        Text suit = new Text(splits[0]);
        context.write(suit, rank);
    }
}

public static class CardCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{   
    public void reduce(Text key, Iterable<IntWritable> value, Context context)
    throws IOException, InterruptedException {
        int[] temp =new int[]{ 1,2,3,4,5,6,7,8,9,10,11,12,13};
    	ArrayList<Integer> ranks = new ArrayList<Integer>();
    	for (IntWritable val : value) {
    		ranks.add(val.get());
    	}

        for (int i = 1;i <= 13;i++){
            if(!ranks.contains(i))
                context.write(key, new IntWritable(i));
        }
    }    
}

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = new Job(conf, "cardcount");
        job.setJarByClass(CardCount.class);
        job.setMapperClass(CardMapper.class);
        job.setReducerClass(CardCountReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
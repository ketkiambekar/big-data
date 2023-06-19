package Taxiin;
import java.io.IOException;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TaxiinAnalysis extends Configured implements Tool {
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Taxiin");
		job.setJarByClass(getClass());

		// configure output and input source
		TextInputFormat.addInputPath(job, new Path(args[0]));
		job.setInputFormatClass(TextInputFormat.class);

		// configure mapper and reducer
		job.setMapperClass(TaxiinMap.class);
		job.setReducerClass(TaxiinReduce.class);

		// configure output
		TextOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new TaxiinAnalysis(), args);
		System.exit(exitCode);
	}
}

class TaxiinMap extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] lines = value.toString().split(",");
		if (lines.length >= 10) {
			if (lines[19].equals("TaxiIn")) 	//ignore header line
			{	
				//do nothing
			} 
			else 
			{
				if (lines[19].equals("NA")) 
				{
					// do nothing.
				} 
				else 
				{
					if (Integer.parseInt(lines[19]) != 1)
					{
						context.write(new Text(lines[17]), new Text(lines[19]));
					}
				}
			}
		}
	}
}

class TaxiinReduce extends Reducer<Text, Text, Text,Text>{
	public void reduce(Text key,Iterable<Text> Values, Context context) throws IOException,InterruptedException
	{
		Double sum=0.0;
		long count=0;
		if(key.equals(0))
		{
			System.err.println("InMap");
		}
		for(Text value:Values)
		{
			sum=sum+Integer.parseInt(value.toString());
			count++;
		}
		sum=sum/count;
		context.write(new Text(sum.toString()), key);
		}
	}
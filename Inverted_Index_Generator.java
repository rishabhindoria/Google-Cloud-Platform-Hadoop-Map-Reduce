import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class InvertedIndex {
	public static class TokenizerMapper extends Mapper<Object,Text,Text,Text>{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      			StringTokenizer itr = new StringTokenizer(value.toString());
      			Text docid = new Text();
      			Text word = new Text();
      			docid.set(itr.nextToken());
      			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());  
        			context.write(word,docid);
     			 }
    		}
  	}
	public static class IntSumReducer extends Reducer<Text,Text,Text,Text> {
    		public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
      			HashMap<String,Integer> a=new HashMap<String,Integer>();
	  		String op="";
	  		for (Text val : values) {
        			if(a.containsKey(val.toString()))
					a.put(val.toString(),a.get(val.toString())+1);
        			else
					a.put(val.toString(),1);
      			}
      			for(Map.Entry<String,Integer> e:a.entrySet())
				op+=e.getKey()+":"+e.getValue()+"\t";
      			context.write(key,new Text(op));
    		}
  	}
  	public static void main(String[] args) throws Exception {
    		Job job=new Job();
    		job.setJobName("Inverted Index Program");
    		job.setJarByClass(InvertedIndex.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job,new Path(args[0]));
    		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(TokenizerMapper.class);
    		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
    		job.setOutputValueClass(Text.class);
    		job.waitForCompletion(true);
  	}	
}

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//package org.apache.hadoop.examples;
 
import java.io.IOException;
import java.util.StringTokenizer;
import java.nio.ByteBuffer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable.Comparator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
public class OutDegree
{

	public static class OutDegreeMapper 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] parts = oneLine.split(" ");
			word.set(parts[1]);
			context.write(word, one);
		}
	}
  
	public static class OutDegreeReducer 
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			int sum = 0;
			for (IntWritable val : values) 
			{
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static class IntComparator extends WritableComparator {

     public IntComparator() {
         super(IntWritable.class);
     }

     @Override
     public int compare(byte[] b1, int s1, int l1,
             byte[] b2, int s2, int l2) {

         Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
         Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

         return v1.compareTo(v2) * (-1);
     	}	
 	}


 	public static class MapTask extends
   	Mapper<LongWritable, Text, IntWritable, IntWritable> {
  		public void map(LongWritable key, Text value, Context context)
   		throws java.io.IOException, InterruptedException {
   		String line = value.toString();
   		String[] tokens = line.split("\t"); // This is the delimiter between
   		int keypart = Integer.parseInt(tokens[0]);
   		int valuePart = Integer.parseInt(tokens[1]);
   		context.write(new IntWritable(valuePart), new IntWritable(keypart));
	  	}
	}

	public static class ReduceTask extends
   	Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
   		int num =0;
   		int count =0;

  		public void reduce(IntWritable key, Iterable<IntWritable> list, Context context)
    	throws java.io.IOException, InterruptedException {
   
    	Configuration conf = context.getConfiguration();
		String params = conf.get("case");   		
		
		num = Integer.parseInt(params);

   		for (IntWritable value : list) {
    
    		if(count <num){
   			context.write(value,key);
    		count++;
    		}
   		}
   
 		}
 	}

	public static void main(String[] args) throws Exception 
	{
		//outdegree count
		Configuration conf = new Configuration();
		String [] fileName = args[0].split("/");
      	int len = fileName.length;
      	int nums=0;

      	if(fileName[len-1].contains("case1")||fileName[len-1].contains("edges.txt"))
      		nums = 2;
      	else if(fileName[len-1].contains("case2")||fileName[len-1].contains("case3"))
      		nums = 20;
      	else{
      		System.err.println("Invalid input!");
			System.exit(2);
      	}

      	conf.set("case",nums+"");
		
		Job job = new Job(conf, "outdegree");
		job.setJarByClass(OutDegree.class);
		job.setMapperClass(OutDegreeMapper.class);
		job.setCombinerClass(OutDegreeReducer.class);
		job.setReducerClass(OutDegreeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,new Path("outputOD-"+fileName[len-1]+"-unsorted"));

		int code = job.waitForCompletion(true) ? 0 : 1;
		//System.out.println("Kanjairs"+code);

		
		//sorting

		Job job2 = new Job(conf, "sorting");
  		job2.setJarByClass(OutDegree.class);

  		job2.setMapperClass(OutDegree.MapTask.class);
  		job2.setReducerClass(OutDegree.ReduceTask.class);
  		job2.setNumReduceTasks(1);

  		job2.setMapOutputKeyClass(IntWritable.class);
  		job2.setMapOutputValueClass(IntWritable.class);
  		job2.setOutputKeyClass(IntWritable.class);
  		job2.setOutputValueClass(IntWritable.class);
  		job2.setSortComparatorClass(IntComparator.class);


  		Path inputPath = new Path(("outputOD-"+fileName[len-1]+"-unsorted"));
  		FileInputFormat.addInputPath(job2, inputPath);
 	 	job2.setInputFormatClass(TextInputFormat.class);


 	 	Path outputDir = new Path(("outputOD-"+fileName[len-1]+"-Final"));
 	 	FileOutputFormat.setOutputPath(job2, outputDir);
  		job2.setOutputFormatClass(TextOutputFormat.class);

  		//biggest out degree vertices

		System.exit(job2.waitForCompletion(true) ? 0 : 1);

		
		
	}
}
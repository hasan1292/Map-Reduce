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
 import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counters;
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
import java.util.StringTokenizer;
public class BFS
{
	//public static int distance = 1;

	public static enum MATCH_COUNTER {
  		INCOMING_GRAPHS,
 		PRUNING_BY_NCV,
  		PRUNING_BY_COUNT,
  		PRUNING_BY_ISO,
  		ISOMORPHIC
	};


	public static class OutDegreeMapper1 
		extends Mapper<Object, Text, Text, Text>
	{
    
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word2 = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] parts = oneLine.split(" ");
			word.set(parts[1]);
			String join = "from1x"+parts[2];
			word2.set(join);
			context.write(word, word2);
		}
	}

	public static class OutDegreeMapper2 
		extends Mapper<Object, Text, Text, Text>
	{
    
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		private Text word2 = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] parts = oneLine.split("\t");
			word.set(parts[0]);
			String join = "from2x"+parts[1];
			word2.set(join);

			context.write(word, word2);
		}
	}
  
	public static class OutDegreeReducer 
		extends Reducer<Text,Text,Text,Text> 
	{
		//private IntWritable result = new IntWritable();
		private Text chaabi = new Text();
		private Text value = new Text();

		public void reduce(Text key, Iterable<Text> values, 
									Context context
							) throws IOException, InterruptedException 
		{
		Configuration conf = context.getConfiguration();
		String param = conf.get("test");
			
			String merge ="";

 			for(Text val:values)
 			{

  				String s = val.toString();
  				merge+= s+":";
  				//if(s.contains("from2")){
  				//String parts[] = s.split("x");

  				//}
 			}



 			if(merge.contains("from2")){
 				String parts[] = merge.split(":");

 				int least = 100;

 				for(int i=0;i<parts.length;i++){
 					if(parts[i].contains("from2")){
 						String []x= parts[i].split("x");
 						if(least> Integer.parseInt(x[1]))
 							least = Integer.parseInt(x[1]);
 					}
 					if(parts[i].contains("from1")){
 						String []x= parts[i].split("x");
 						chaabi.set(x[1]);
 						//String num = String.valueOf(N);
 						value.set(param);
 						context.write(chaabi,value);

 					}
 				}

 				String num = String.valueOf(least);
 				value.set(num);
 				context.write(key, value);
 				
 			}
 			else{
 				context.getCounter(MATCH_COUNTER.INCOMING_GRAPHS).increment(1);
 			}
  		}
	}

	public static class OutDegreeMapperAlpha 
		extends Mapper<Object, Text, Text, IntWritable>
	{
    
		private IntWritable word2 = new IntWritable();
		private Text word = new Text();
		//private Text word2 = new Text();
      
		public void map(Object key, Text value, Context context
							) throws IOException, InterruptedException 
		{
			String oneLine = value.toString();
			String[] parts = oneLine.split("\t");
			word.set(parts[0]);
			word2.set(Integer.parseInt(parts[1]));

			context.write(word, word2);
		}
	}

	public static class OutDegreeReducerAlpha 
		extends Reducer<Text,IntWritable,Text,IntWritable> 
	{
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, 
									Context context
							) throws IOException, InterruptedException 
		{
			int least=100;
			for (IntWritable val : values) 
			{
				if(least > val.get())
						least = val.get();
				
				
			}
			result.set(least);
			context.write(key, result);
		}
	}

		public static class IntComparator2 extends WritableComparator {

     public IntComparator2() {
         super(IntWritable.class);
     }

     @Override
     public int compare(byte[] b1, int s1, int l1,
             byte[] b2, int s2, int l2) {

         Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
         Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();

         return v1.compareTo(v2) * (1);
     	}	
 	}


 	public static class MapTask2 extends
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

	public static class ReduceTask2 extends
   	Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
  		public void reduce(IntWritable key, Iterable<IntWritable> list, Context context)
    	throws java.io.IOException, InterruptedException {
   
   		for (IntWritable value : list) {
    
   			context.write(value,key);
    
   		}
   
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

   		private IntWritable result = new IntWritable(0);
  		public void reduce(IntWritable key, Iterable<IntWritable> list, Context context)
    	throws java.io.IOException, InterruptedException {
   
    	//String param = conf.get("case");


   		for (IntWritable value : list) {

   			context.write(value,result);

   		}
   
 		}
 	}

	public static void main(String[] args) throws Exception 
	{
		//outdegree count
		
		//String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		
      	Configuration conf = new Configuration();

      	String [] fileName = args[0].split("/");
      	int len = fileName.length;

      	if(fileName[len-1].contains("case1")||fileName[len-1].contains("edges.txt"));
      	else if(fileName[len-1].contains("case2")||fileName[len-1].contains("case3"));
      	else{
      		System.err.println("Invalid input!");
			System.exit(2);
      	}

      	Job jobX = new Job(conf, "gettingZeros");
  		jobX.setJarByClass(BFS.class);


  		jobX.setMapperClass(BFS.MapTask.class);
  		jobX.setReducerClass(BFS.ReduceTask.class);
  		jobX.setNumReduceTasks(1);

  		jobX.setMapOutputKeyClass(IntWritable.class);
  		jobX.setMapOutputValueClass(IntWritable.class);
  		jobX.setOutputKeyClass(IntWritable.class);
  		jobX.setOutputValueClass(IntWritable.class);
  		jobX.setSortComparatorClass(IntComparator.class);


  		Path inputPathX = new Path("outputOD-"+fileName[len-1]+"-Final");
  		FileInputFormat.addInputPath(jobX, inputPathX);
 	 	jobX.setInputFormatClass(TextInputFormat.class);


 	 	Path outputDirX = new Path("outputBFS-"+fileName[len-1]+"-" + 0);
 	 	FileOutputFormat.setOutputPath(jobX, outputDirX);
  		jobX.setOutputFormatClass(TextOutputFormat.class);



  		
  		int codeX = jobX.waitForCompletion(true) ? 0 : 1;

  		


		Path p1 = new Path(args[0]);
		//Path p2 = new Path("outputwc3");

		int i=0;
		Boolean check = false;
		long previous = 0l;

		while(!check){

		String input;
      	if (i == 0)
        	input = "outputBFS-"+fileName[len-1]+"-" + i;
      	else
        	input = "outputBFS-"+fileName[len-1]+"-" + i;

      	String output = "outputBFS-"+fileName[len-1]+"-"+(i+1);

      	Path p2 = new Path(input);

      	conf.set("test",(i+1)+"");
		Job job = new Job(conf, "BFS");
		job.setJarByClass(BFS.class);

		//job.setMapperClass(OutDegreeMapper.class);

		MultipleInputs.addInputPath(job, p1, TextInputFormat.class, OutDegreeMapper1.class);
		MultipleInputs.addInputPath(job, p2, TextInputFormat.class, OutDegreeMapper2.class);

		//job.setCombinerClass(OutDegreeReducer.class);
		job.setReducerClass(OutDegreeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//for (int i = 0; i < otherArgs.length - 1; ++i) 
		//{
		//	FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		//}


		FileOutputFormat.setOutputPath(job, new Path(output));
		int code = job.waitForCompletion(true) ? 0 : 1;
		Counters counters = job.getCounters();
		//Counter c1 = counters.findCounter(MATCH_COUNTER.INCOMING_GRAPHS);
		System.out.println("Kak:"+counters.findCounter(MATCH_COUNTER.INCOMING_GRAPHS).getValue());
		if(counters.findCounter(MATCH_COUNTER.INCOMING_GRAPHS).getValue()==0)
		check = true;
		if(counters.findCounter(MATCH_COUNTER.INCOMING_GRAPHS).getValue()==previous)
		check= true;
		previous = counters.findCounter(MATCH_COUNTER.INCOMING_GRAPHS).getValue();
		
		i++;
		//distance++;
		//System.out.println("Kanjairs"+code);
		}
		
		//Configuration conf = new Configuration();

		Job job2 = new Job(conf, "reducing");
  		job2.setJarByClass(BFS.class);

  		job2.setMapperClass(BFS.OutDegreeMapperAlpha.class);
  		job2.setReducerClass(BFS.OutDegreeReducerAlpha.class);
  		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job2, new Path("outputBFS-"+fileName[len-1]+"-"+i));
		FileOutputFormat.setOutputPath(job2, new Path("outputBFS-"+fileName[len-1]+"-Red"));

		int code = job2.waitForCompletion(true) ? 0 : 1;
		

		//sorting the final data now
		Job job3 = new Job(conf, "sorting");
  		job3.setJarByClass(BFS.class);

  		job3.setMapperClass(BFS.MapTask2.class);
  		job3.setReducerClass(BFS.ReduceTask2.class);
  		job3.setNumReduceTasks(1);

  		job3.setMapOutputKeyClass(IntWritable.class);
  		job3.setMapOutputValueClass(IntWritable.class);
  		job3.setOutputKeyClass(IntWritable.class);
  		job3.setOutputValueClass(IntWritable.class);
  		job3.setSortComparatorClass(IntComparator2.class);


  		Path inputPath = new Path("outputBFS-"+fileName[len-1]+"-Red");
  		FileInputFormat.addInputPath(job3, inputPath);
 	 	job3.setInputFormatClass(TextInputFormat.class);


 	 	Path outputDir = new Path("outputBFS-"+fileName[len-1]+"-Final");
 	 	FileOutputFormat.setOutputPath(job3, outputDir);
  		job3.setOutputFormatClass(TextOutputFormat.class);


		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		
		
	}
}
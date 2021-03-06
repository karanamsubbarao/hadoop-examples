package com.training.mapreduce.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException 
	{
		int count =0;
		for (IntWritable value : values) {
			System.out.println("Value..." + value);
			count++;
		}
		context.write(key, new IntWritable(count));
		
	}

}

package com.training.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> 
{
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException 
	{
		String line = value.toString();
		for (String word : line.split("\\W+")) {
		if (word.length() > 0) {
        	context.write(new Text(word), new IntWritable(1));
		}	
	};

}

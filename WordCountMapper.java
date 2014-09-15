package com.training.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, Text> 
{
	
	protected void map(LongWritable key, Text value, Context context) throws java.io.IOException ,InterruptedException 
	{
		String line = value.toString();
		String[] words = line.split(",");
		for(int i=1;i<words.length;i++)
		{
			context.write(new Text(words[i]),new Text(words[0]));
		}
			
	};

}

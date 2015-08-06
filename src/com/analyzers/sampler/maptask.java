package com.analyzers.sampler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.omg.CORBA.portable.ValueOutputStream;

public class maptask extends
Mapper<LongWritable, Text, Text, Text>{
	int c=0;
	private String yourArgument;

	//to set custom values;
	 /* @Override
	protected void setup(Context context) 
	{
		Configuration c = context.getConfiguration();
		yourArgument = c.get("yourcustom.property");
		int a= Integer.parseInt(yourArgument)+1;
		c.set("yourcustom.property",Integer.toString(a));
		yourArgument = c.get("yourcustom.property");
	}*/
	
	public void map(LongWritable mapperInputKey, Text mapperInputValue,
			Context context) throws IOException, InterruptedException 
	{
		//System.out.print(yourArgument);
		++c;
		/*int a= Integer.parseInt(yourArgument);*/
		//System.out.println(yourArgument);
		String line = mapperInputValue.toString();
		String[] values = line.split(",");
		String val=values[1]+","+values[2];
		if(c%4==0)
		{
		context.write(new Text(values[0]), new Text(val));
		}
	}

}

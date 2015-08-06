package com.analyzers.sampler;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class reducetask extends
Reducer<Text, Text, Text, Text> 
{
	//public static int q;
	int maxtemp=0;
	int[] arrpara=new int[20];
	 int count=0;
	int[] maxpara=new int[20];
	int length1;
	private String yourArgument;
	protected void setup(Context context) {
        Configuration c = context.getConfiguration();
        yourArgument = c.get("yourcustom.property");
        System.out.println(yourArgument);
      //put here
		
      		/*String[] para=yourArgument.toString().split("0");
      		length1=para.length;
      		length1=(length1/2)+1;
      		for(String par : para)
      		{
      			arrpara[count]=Integer.parseInt(par);
      			count++;
      			System.out.println(par);
      			
      		}*/
      		//end here
      		 
      		 
    }
	/*int maxhumidity=0;*/
	public void reduce(Text key, Iterable<Text> values,
			Context context) throws IOException, InterruptedException 
	{
		//toolrunner obj=new toolrunner();
		//System.out.println(recudertask.q);
		//int c = 0;
		//int f = 0;
		//System.out.println(key.toString());
		//System.out.println(obj.word());
			//System.out.print("hellp");
		int flag=0;
		//System.out.print(yourArgument);
		int a= Integer.parseInt(yourArgument);
		for(Text value : values) 
		{
			String[] f=value.toString().split(",");
			/*for(int par : arrpara)
      		{
      			System.out.println(par);
      			int a = Integer.parseInt(f[par]);
      			if(maxpara[par]<a)
      			{
      				maxpara[par]=a;
      			}
      			
      		}*/
			
			//int temp = Integer.parseInt(f[a]);
			int temp = Integer.parseInt(f[a]);
			//int humidity= Integer.parseInt(f[1]);
			if(temp>maxtemp )
			{
				flag=1;
				maxtemp=temp;
			}
		}
		/*String max="";
		for(int index : arrpara)
		{
			max=max+(Integer.toString(maxpara[index]))+" ";
		}*/
		if(flag==1)
		{
		context.write(key,new Text(Integer.toString(maxtemp)));
		}
	}

}


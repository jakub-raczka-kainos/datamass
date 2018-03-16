package io.datamass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapWordCount extends Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException
    {
        String line = value.toString();
        String[] words=line.split("\\t");
        for (int col = 0; col < words.length; col++ ) {
            String word = words[col];
            Text outputValue = new Text(word.trim());
            IntWritable outputKey = new IntWritable(col);
            con.write(outputKey, outputValue);
        }
    }

}

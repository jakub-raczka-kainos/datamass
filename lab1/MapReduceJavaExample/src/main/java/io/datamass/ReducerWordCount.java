package io.datamass;


import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


public class ReducerWordCount extends Reducer<IntWritable, Text, IntWritable, Text>
{
    public void reduce(IntWritable column, Iterable<Text> values, Context con) throws IOException, InterruptedException
    {
        Map<Text, Integer> map = new HashMap<Text, Integer>();
        int counter = 0;
        for (Text word : values)
        {
            int count = map.containsKey(word) ? map.get(word) : 0;
            map.put(new Text(word), count + 1);
            counter++;

        }
        for (Map.Entry<Text, Integer> entry : map.entrySet()) {
            String out = String.format("WORD: %s  COUNT: %s [%.2f %%]",
                    entry.getKey().toString(),
                    entry.getValue().toString(),
                    entry.getValue() * 100F / counter);
            con.write(column, new Text(out));
        }

    }
}


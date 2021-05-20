

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Set;

public class Task1_2_Stripes {

    public static class MapperRelativeStripes extends Mapper<LongWritable, Text, Text, MapWritable> {

        private MapWritable wordOccurrenceList = new MapWritable();
        private Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //removing the symbols from the text line using replaceAll method
            //splitting the text line using regex.
            String[] tokens = value.toString().trim().replaceAll("[^a-zA-Z0-9\\s]","").split("\\s+");

            //if the text line is greater than one word then
            if (tokens.length > 1) {

                //iterating over the words present in the text line
                for (int i = 0; i < tokens.length; i++) {

                    wordOccurrenceList.clear();

                    //setting the word in wordpair class as key
                    word.set(tokens[i].trim());

                    //iterating over the words which are neighbour present in the text line
                    for (int j = 0; j < tokens.length; j++) {

                        //if the key word and neighbour is not equal
                        //then setting the word as neighbour for the key word
                        if (i != j) {

                            //if the map contains the neighbour then add its count.
                            if (wordOccurrenceList.containsKey(new Text(tokens[j].trim()))) {
                                DoubleWritable count = (DoubleWritable) wordOccurrenceList.get(new Text(tokens[j].trim()));
                                count.set(count.get() + 1);
                            }
                            else

                                //if the neighbour is not present in the map then add the neighbour and its count
                                wordOccurrenceList.put((new Text(tokens[j].trim())), new DoubleWritable(1));
                        }
                    }

                    double sum = 0.0;
                    Set<Writable> keys =wordOccurrenceList.keySet();

                    //Calculating total value
                    for(Writable totalValue:keys)
                    {
                        DoubleWritable temp =(DoubleWritable) wordOccurrenceList.get(totalValue);
                        sum = sum + temp.get();
                    }

                    //* symbol indicates the total value
                    wordOccurrenceList.put(new Text("*"),new DoubleWritable(sum));

                    //emitting the word and its neighbour along with occurrence
                    context.write(word, wordOccurrenceList);
                }
            }
        }
    }

    public static class ReducerRelativeStripes extends Reducer<Text, MapWritable, Text, MapWritable> {

        private Text symbol = new Text("*");
        MapWritable totalWordCountMap = new MapWritable();


        @Override
        protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {

            totalWordCountMap.clear();

            for (MapWritable value : values) {
                addAll(value, totalWordCountMap);
            }

            //fetching the value of total count i.e * value of the word
            DoubleWritable symbolValues = (DoubleWritable) totalWordCountMap.get(symbol);
            double var = symbolValues.get();

            for (Writable index : totalWordCountMap.keySet()) {
                Text x = (Text) index;

                if (!x.equals(symbol)) {

                    //compute the relative frequency by dividing the count by total value
                    DoubleWritable y = (DoubleWritable) totalWordCountMap.get(index);
                    totalWordCountMap.put(index, new DoubleWritable((double) y.get() / var));
                }


            }

            context.write(key, totalWordCountMap);
        }

        //addAllValues counts the frequency of the words
    private void addAll(MapWritable mapWritable, MapWritable occurrenceMap) {

        Set<Writable> keys = mapWritable.keySet();

        for (Writable index : keys ) {

            //fetch the neighbour word count value
            DoubleWritable fromCount = (DoubleWritable) mapWritable.get(index);

            //if map contain the key then calculate the total count.
            if (occurrenceMap.containsKey(index)) {

                DoubleWritable count = (DoubleWritable) occurrenceMap.get(index);
                count.set(count.get() + fromCount.get());
            }
            else {

                //if the map does not contain the key then insert the key.
                occurrenceMap.put(index, fromCount);

            }
        }
    }
}


    public static void main(String[] args) throws Exception
    {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Task 1.2_Stripes");
        job.setJarByClass(Task1_1_Stripes.class);

        job.setMapperClass(MapperRelativeStripes.class);
        job.setReducerClass(ReducerRelativeStripes.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

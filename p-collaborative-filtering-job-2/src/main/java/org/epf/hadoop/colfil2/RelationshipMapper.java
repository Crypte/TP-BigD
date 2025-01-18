package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class RelationshipMapper extends Mapper<LongWritable, Text, UserPair, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\\t");
        if (parts.length != 2) {
            return;
        }

        String user = parts[0];
        List<String> friends = Arrays.asList(parts[1].split(", "));
        for (int i = 0; i < friends.size(); i++) {
            for (int j = i + 1; j < friends.size(); j++) {
                UserPair pair = new UserPair(friends.get(i), friends.get(j));
                context.write(pair, new Text(user));
            }
        }

        for (String friend : friends) {
            UserPair directPair = new UserPair(user, friend);
            context.write(directPair, new Text("DIRECT"));
        }
    }
}
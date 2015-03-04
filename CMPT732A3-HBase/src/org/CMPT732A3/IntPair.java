package org.CMPT732A3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
 
public class IntPair implements WritableComparable{
 
    private IntWritable first;
    private IntWritable second;
    
    
 
    public IntPair(IntWritable first, IntWritable second) {
        set(first, second);
    }
 
    public IntPair() {
        set(new IntWritable(), new IntWritable());
    }
 
    public IntPair(int first, int second) {
        set(new IntWritable(first), new IntWritable(second));
    }
 
    
    public IntWritable getFirst() {
        return this.first;
    }
 
    public IntWritable getSecond() {
        return this.second;
    }
 
    public void set(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
    }
 
    @Override
    public String toString() {
        return this.first.toString() + ":" + this.second.toString();
    }
 
    @Override
    public int compareTo( Object o) {
    	IntPair tp = (IntPair) o;
        int cmp = first.compareTo(tp.first);
 
        if (cmp != 0) {
            return cmp;
        }
 
        return second.compareTo(tp.second);
    }
 
    @Override
    public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
    }
 
    @Override
    public boolean equals(Object o)
    {
        if(o instanceof IntPair)
        {
        	IntPair tp = (IntPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
 
}


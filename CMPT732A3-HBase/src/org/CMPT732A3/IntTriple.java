package org.CMPT732A3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
 
public class IntTriple implements WritableComparable{
 
    private IntWritable first;
    private IntWritable second;
    private IntWritable third;

    
    
 
    public IntTriple(IntWritable first, IntWritable second,IntWritable third) {
        set(first, second,third);
    }
 
    public IntTriple() {
        set(new IntWritable(), new IntWritable(),new IntWritable());
    }
 
    public IntTriple(int first, int second,int third) {
        set(new IntWritable(first), new IntWritable(second),new IntWritable(third));
    }
 
    
    public IntWritable getFirst() {
        return this.first;
    }
 
    public IntWritable getSecond() {
        return this.second;
    }
    
    public IntWritable getThird() {
        return this.third;
    }
    
 
    public void set(IntWritable first, IntWritable second, IntWritable third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        third.readFields(in);
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        third.write(out);
    }
 
    @Override
    public String toString() {
        return this.first.toString() + ":" + this.second.toString() + ":" + this.third.toString();
    }
 
    @Override
    public int compareTo( Object o) {
    	IntTriple tp = (IntTriple) o;
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
        if(o instanceof IntTriple)
        {
        	IntTriple tp = (IntTriple) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
 
}


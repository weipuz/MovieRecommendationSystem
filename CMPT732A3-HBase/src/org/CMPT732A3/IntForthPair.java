package org.CMPT732A3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
 
public class IntForthPair implements WritableComparable{
 
    private IntWritable first;
    private IntWritable second;
    private IntWritable third;
    private IntWritable forth;

    
    
 
    public IntForthPair(IntWritable first, IntWritable second,IntWritable third,IntWritable forth) {
        set(first.get(), second.get(),third.get(),forth.get());
    }
 
    public IntForthPair() {
        first = new IntWritable();
        second = new IntWritable();
        third = new IntWritable();
        forth = new IntWritable();
    }
 
    public IntForthPair(int first, int second,int third,int forth) {
        set(first, second,third,forth);
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
    
    public IntWritable getForth() {
        return this.forth;
    }
    
 
    public void set(int first, int second, int third,int forth) {
        this.first = new IntWritable(first);
        this.second = new IntWritable(second);
        this.third = new IntWritable(third);
        this.forth = new IntWritable(forth);
    }
 
    @Override
    public void readFields(DataInput in) throws IOException {
        first.readFields(in);
        second.readFields(in);
        third.readFields(in);
        forth.readFields(in);
    }
 
    @Override
    public void write(DataOutput out) throws IOException {
        first.write(out);
        second.write(out);
        third.write(out);
        forth.write(out);
    }
 
    @Override
    public String toString() {
        return this.first.toString() + ":" + this.second.toString() + ":" + this.third.toString() +  ":" + this.forth.toString();
    }
 
    @Override
    public int compareTo( Object o) {
    	IntForthPair tp = (IntForthPair) o;
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
        if(o instanceof IntForthPair)
        {
        	IntForthPair tp = (IntForthPair) o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
    }
 
}


package org.CMPT732A3;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;

public class textbytes {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		byte [] row1 = Bytes.toBytes("12343" + ":" + "12341234");
		
		System.out.println(row1.toString());
		String s = new String(row1, "UTF-8");
		System.out.println(s);

	}

}

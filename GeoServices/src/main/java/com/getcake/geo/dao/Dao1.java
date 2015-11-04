package com.getcake.geo.dao;

import java.util.ArrayList;
import java.util.Arrays;

public class Dao1<T extends Comparable<T>> {

    public T get(int index) {
        

        return null;
    }
	
	public T  maximum(T x, T y, T z)
	   {                     
		  ArrayList a;
	      T max = x; // assume x is initially the largest       
	      if ( y.compareTo( max ) > 0 ){
	         max = y; // y is the largest so far
	      }
	      if ( z.compareTo( max ) > 0 ){
	         max = z; // z is the largest now                 
	      }
	      return max; // returns the largest object   
	   }	
	
	   public static <A extends Comparable<A>> A maximum2 (A x, A y, A z)
	   {                      
	      A max = x; // assume x is initially the largest       
	      if ( y.compareTo( max ) > 0 ){
	         max = y; // y is the largest so far
	      }
	      if ( z.compareTo( max ) > 0 ){
	         max = z; // z is the largest now                 
	      }
	      return max; // returns the largest object   
	   }
	   
	   public static < E > void printArray( E[] inputArray )
	   {
	      // Display array elements              
	         for ( E element : inputArray ){        
	            System.out.printf( "%s ", element );
	         }
	         System.out.println();
	    }
	   
	public static void main (String args[]) {
		Dao1<Integer> dao1 = new Dao1();
		
		dao1.maximum(4, 5, 6);
		Arrays.asList(dao1);
		
		Dao1<Double> dao2 = new Dao1();
		
		dao2.maximum(7.0, 5.0, 3.0);
		
	}
}

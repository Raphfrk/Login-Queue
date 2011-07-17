package com.raphfrk.bukkit.loginqueue2;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Logger;


public class MiscUtils {
	
	final static private Logger log = Logger.getLogger("Minecraft");
	
	static void stringToFile( ArrayList<String> string , String filename ) {
		
		
		File portalFile = new File( filename );

		BufferedWriter bw;
		
		try {
			bw = new BufferedWriter(new FileWriter(portalFile));
		} catch (FileNotFoundException fnfe ) {
			log.info("[LoginQueue] Unable to write to property file: " + filename );
			return;
		} catch (IOException ioe) {
			log.info("LoginQueue] Unable to write to property file: " + filename );
			return;
		}
		
		try {
			for( Object line : string.toArray() ) {
				bw.write((String)line);
				bw.newLine();
			}
			bw.close();
		} catch (IOException ioe) {
			log.info("[LoginQueue] Unable to write to property file: " + filename );
			return;
		}
		
	}
	
	static String[] fileToString( String filename ) {
		
		File portalFile = new File( filename );
		
		BufferedReader br;
		
		try {
			br = new BufferedReader(new FileReader(portalFile));
		} catch (FileNotFoundException fnfe ) {
			log.info("[LoginQueue] Unable to open property file: " + filename );
			return null;
		} 
		
		StringBuffer sb = new StringBuffer();
		
		String line;
		
		try {
		while( (line=br.readLine()) != null ) {
			sb.append( line );
			sb.append( "\n" );
			
		}
		br.close();
		} catch (IOException ioe) {
			log.info("[LoginQueue] Error reading file: " + filename );
			return null;
		}
		
		return( sb.toString().split("\n") );
	}
	

}

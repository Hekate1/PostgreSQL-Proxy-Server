package com.atscale.proxy.server;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;

import ch.qos.logback.classic.net.SyslogAppender;

public class Server {
	
	private static byte[] PARSE_COMP = {49,0,0,0,4};
	private static byte[] BIND_COMP = {50,0,0,0,4};
	private static byte[] READY = {90,0,0,0,5,73};
	
	
    public static void main(String[] args) throws FileNotFoundException, SQLException, ClassNotFoundException {
    	try {
    		boolean connected = false;
    		
        	System.out.println("Here");
            ServerSocket sSocket = new ServerSocket(5433);
            Socket cSocket = sSocket.accept();
            DataOutputStream out = new DataOutputStream(cSocket.getOutputStream());
            BufferedReader in = new BufferedReader(new InputStreamReader(cSocket.getInputStream()));
            
            ArrayList<String> output = new ArrayList<String>();
            Random r = new Random();
            output.add("Server Working" + r.nextInt());
        	Path file = Paths.get("log.txt");
        	
        	Files.write(file, output, StandardOpenOption.APPEND);
            output.clear();
            
    		File inputFile = new File("rep1.txt");
            Scanner sc = new Scanner(inputFile);
            
            byte[] rep1 = new byte[322];
            
            for(int i = 0; i < 322; i++)
            {
            	rep1[i] = (byte)Integer.parseInt(sc.nextLine());
            }
            
            Class.forName("org.postgresql.Driver");
            
            Connection con = DriverManager.getConnection("jdbc:postgresql://localhost");
            
            Statement statement = con.createStatement();
            ResultSet rs = null;
            
        	while(cSocket.isClosed() == false)
        	{
        		// make buffer of size x
        		// read 
        		char[] buffer = new char[100];
        		int bytesRead = in.read(buffer);
        		if(bytesRead > 0)
        		{
        			output.add(new String(buffer));
                    Files.write(file, output, StandardOpenOption.APPEND);
                    System.out.println("Wrote to file");
        			
        			if(buffer[8] == 'u' && connected == false)
        			{	
        	            out.write(rep1);
        	            connected = true;
        	            System.out.println("Repsponded");
        			}
        			else if (buffer[0] == 88)//Termination Command
        			{
        				System.out.println("Terminating");
        				cSocket.close();
        			}
        			else
        			{
        				String query = "";
        				for(char c : buffer)
        				{
        					if(c > 31) //32 and above are normal characters
        					{
        						query = query.concat(Character.toString(c));
        					}
        				}
        				if(query.lastIndexOf('B') != -1)
        				{
            				query = query.substring(1, query.lastIndexOf('B'));
        				}
        				rs = statement.executeQuery(query);
        				short columnCount = (short) rs.getMetaData().getColumnCount();
        				System.out.println("Data Recived");
        				
        				//ROW DESC CREATION
        				byte [] temp = ByteBuffer.allocate(2).putShort(columnCount).array();
        				byte[] column;
        				char[] chars;
        				byte[] name;
        				int length = 4 + 2;
        				for(int i = 1; i <= columnCount; i++)
        				{
        					chars = rs.getMetaData().getColumnName(i).toCharArray();
        					name = new byte[chars.length + 1];
        					for(int j = 0; j < chars.length; j++)
        					{
        						name[j] = (byte)chars[j];
        					}
        					name[chars.length] = 0; //Terminating null at end of string
        					column = merge(new byte[][] {name, new byte[] {0,0,0,0}/*TableOID*/,new byte[] {0,(byte) i},new byte[] {0,0,0,0}/*Data type OID*/});
        					
        					if(rs.getMetaData().getColumnTypeName(i) == "int4")
        					{
        						column = merge(new byte[][] {column, new byte[] {0,4}});
        					}
        					else if(rs.getMetaData().getColumnTypeName(i) == "varchar")
        					{
        						column = merge(new byte[][] {column, ByteBuffer.allocate(2).putShort((short)255).array()});
        					}
        					
        					column = merge(new byte[][] {column, new byte[] {(byte) 0xff,(byte) 0xff,(byte) 0xff,(byte) 0xff},new byte[] {0,0}});
        					temp = merge(new byte[][] {temp,column});
        					length += name.length + 4 + 2 + 4 + 2 + 4 + 2;
        				}
        				System.out.println(length);
        				byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
        				byte[] rowDesc = merge(new  byte[][] {new byte[] {84}, byteLength, temp});
        				//END ROW DESC CREATION
        				//DATA ROW CREATION (prob stick in loop for each row?)
        				int rowCounter = 0;
        				byte[] dataRow = new byte[0];
        				while(rs.next())
        				{
        					rowCounter++;
        					byte[] drId = new byte[] {'D'};
            				length = 4 + 2;
            				
            				byte[][] columns = new byte[columnCount][];
            				for(int i = 0; i < columnCount; i++) //Fix to deal with null (length = -1 with no data) and other values
            				{
            					if(rs.getMetaData().getColumnTypeName(i+1) == "int4" || rs.getMetaData().getColumnTypeName(i+1) == "varchar")
            					{
            						String data = rs.getObject(i+1).toString();
            						int colLength = data.length();
            						length += colLength;
            						byte[] byteColLength = ByteBuffer.allocate(4).putInt(colLength).array();
            						columns[i] = new byte[data.length()];
            						for(int j = 0; j < data.length(); j++)
            						{
            							columns[i][j] = (byte)data.charAt(j);
            						}
            						columns[i] = merge(new byte[][] {byteColLength, columns[i]});
            					}
            				}
            				byte[] colData = merge(columns);
            				byteLength = ByteBuffer.allocate(4).putInt(length).array();
            				byte [] byteColumnCount = ByteBuffer.allocate(2).putShort(columnCount).array();
            				dataRow = merge(new byte[][] {dataRow, drId,byteLength,byteColumnCount,colData});
        				}
        				//END DATA ROW CREATION
        				
        				//COMMAND COMPLETION CREATION
        				byte[] ccId = new byte[] {'C'};
        				String command = query.substring(0, query.indexOf(' ')).toUpperCase();
        				char[] charCommand = command.toCharArray();
        				String strRowCount = Integer.toString(rowCounter);
        				byte[] byteCommand = new byte[charCommand.length + 1 + strRowCount.length() + 1];
        				for(int i = 0; i < charCommand.length; i++)
        				{
        					byteCommand[i] = (byte)charCommand[i];
        				}
        				byteCommand[charCommand.length] = 32; //Space
        				for(int i = 0; i < strRowCount.length(); i++)
        				{
        					byteCommand[i + 1 + charCommand.length] = (byte)strRowCount.charAt(i);
        				}
        				byteCommand[byteCommand.length-1] = 0;
        				length = 4 + byteCommand.length;
        				byteLength = ByteBuffer.allocate(4).putInt(length).array();
        				byte[] comComp = merge(new byte[][] {ccId,byteLength,byteCommand});
        				//END COMMAND COMPLETION CREATION
        				
        				
        				byte[] combinded = merge(new byte[][] {PARSE_COMP, BIND_COMP, rowDesc, dataRow, comComp, READY});
        				out.write(combinded);
        			}
        		}
        	}
            
            sSocket.close();
            System.out.println("Finished");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static byte[] merge(byte[][] array)
    {
    	int i = 0;
    	int pos = 1;
    	while(pos < array.length)
    	{
    		byte[] combinded = new byte[array[0].length + array[pos].length];
    		for(i = 0; i < array[0].length; i++)
    		{
    			combinded[i] = array[0][i];
    		}
    		for(int j = 0; j < array[pos].length; j++)
    		{
    			combinded[i+j] = array[pos][j];
    		}
    		array[0] = combinded;
    		pos++;
    	}
    	return array[0];
    }
}

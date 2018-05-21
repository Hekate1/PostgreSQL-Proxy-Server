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
import java.util.Random;
import java.util.Scanner;

public class Server {	
    public static void main(String[] args){
    	try {
        	System.out.println("Here");
        	Socket socket = null;
            ServerSocket serverSocket = new ServerSocket(5433);
            
            while (true) {
                try {
                    socket = serverSocket.accept();
                } catch (IOException e) {
                    System.out.println("I/O error: " + e);
                }
                // new thread for a client
                new EchoThread(socket).start();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

final class EchoThread extends Thread{
	
	private static byte[] PARSE_COMP = {49,0,0,0,4};
	private static byte[] BIND_COMP = {50,0,0,0,4};
	private static byte[] READY = {90,0,0,0,5,73};
	
    protected Socket socket;
    
    private int rowCounter = 0;

    public EchoThread(Socket clientSocket) {
        this.socket = clientSocket;
    }

    public void run(){
    	try {
	        System.out.println("New Thread");
	        DataOutputStream out = new DataOutputStream(socket.getOutputStream());
	        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
	        
	        ArrayList<String> output = new ArrayList<String>();
	        Random r = new Random();
	        output.add("Server Working" + r.nextInt());
	    	Path file = Paths.get("log.txt");
	    	
	    	Files.write(file, output, StandardOpenOption.APPEND);
	        output.clear();
	        
			File inputFile = new File("rep2.txt");
	        Scanner sc = new Scanner(inputFile);
	        
	        byte[] rep2 = new byte[315];
	        
	        for(int i = 0; i < 315; i++)
	        {
	        	rep2[i] = (byte)Integer.parseInt(sc.nextLine());
	        }
	        
	        Class.forName("org.postgresql.Driver");
	        
	        Connection con = DriverManager.getConnection("jdbc:postgresql://localhost");
	        
	        Statement statement = con.createStatement();
	        ResultSet rs = null;
	        
	    	while(socket.isClosed() == false)
	    	{
	    		char[] buffer = new char[5000];
	    		int bytesRead = in.read(buffer);
	    		if(bytesRead > 0)
	    		{
	    			System.out.println("Bytes Read");
	    			output.add(new String(buffer));
	                Files.write(file, output, StandardOpenOption.APPEND);
	                System.out.println("Wrote to file");
	                
	                String query = "";
					for(char c : buffer)
					{
						if(c > 31) //32 and above are normal characters
						{
							query = query.concat(Character.toString(c));
						}
					}
	    			
	    			if(buffer[8] == 'u' && buffer[9] == 's' && buffer[10] == 'e' && buffer[11] == 'r')
	    			{	
	    	            out.write(rep2);
	    	            System.out.println("Repsponded");
	    			}
	    			else if(query.contains("SET extra_float_digits"))
	    			{
	    				byte[] efdResponse = new byte[]{49,0,0,0,4,50,0,0,0,4,67,0,0,0,8,83,69,84,0,90,0,0,0,5,73};
	    				
	    				out.write(efdResponse);
	    			}
	    			else if (buffer[0] == 88)//Termination Command
	    			{
	    				System.out.println("Terminating");
	    				socket.close();
	    			}
	    			else
	    			{
	    				int startIndex = -1;
	    				int endIndex = 0;
	    				boolean set = false;
	    				for(int i = 0; i < buffer.length; i++)
	    				{
	    					if(buffer[i] == 'S' && buffer[i+1] == 'E' && set == false) //For Select
	    					{
	    						set = true;
	    						startIndex = i;
	    					}
	    				}
	    				set = false;
	    				if(startIndex == -1)
	    				{
	    					out.write(READY);
	    				}
	    				else
	    				{
	    					for(int i = startIndex; i < buffer.length; i++)
		    				{
		    					if(buffer[i] == (byte)0 && set == false)
		    					{
		    						set = true;
		    						endIndex = i;
		    					}
		    				}
		    				query = "";
		    				for(int i = startIndex; i < endIndex; i++)
		    				{
		    					query = query.concat(Character.toString(buffer[i]));
		    				}
		    				System.out.println(query);
		    				rs = statement.executeQuery(query);
		    				short columnCount = (short) rs.getMetaData().getColumnCount();
		    				System.out.println("Data Recived");
		    				
		    				String flags = "";
		    				for(int i = endIndex; i < buffer.length; i++)
		    				{
		    					flags = flags.concat(Character.toString(buffer[i]));
		    				}
		    				
		    				byte[] combinded;
		    				if(flags.indexOf('E') != -1)
		    				{
		    					//ROW DESC CREATION
			    				byte[] rowDesc = createRowDesc(rs, columnCount);

			    				//DATA ROW CREATION
			    				byte[] dataRow = createDataRows(rs, columnCount);

			    				//COMMAND COMPLETION CREATION
			    				byte[] comComp = createCommandCompletion(query);
			    				
			    				combinded = merge(new byte[][] {PARSE_COMP, BIND_COMP, rowDesc, dataRow, comComp, READY});
		    				}
		    				else
		    				{
		    					//ROW DESC CREATION
			    				byte[] rowDesc = createRowDesc(rs, columnCount);
			    				
			    				combinded = merge(new byte[][] {PARSE_COMP, new byte[]{(byte)'t',0,0,0,6,0,0}, rowDesc, READY}); //Missing ParameterDescription ('t') but prob not needed
		    				}
		    				
		    				out.write(combinded);
	    				}
	    			}
	    		}
	    	}
	        
	        sc.close();
	        System.out.println("Finished");
    	} catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
    }
    
    private byte[] createRowDesc(ResultSet rs, short columnCount) throws SQLException
    {
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
			column = merge(new byte[][] {name, new byte[] {0,0,0,0}/*TableOID*/,new byte[] {0,0},new byte[] {0,0,0,0}/*Data type OID*/});
			
			
			String type = rs.getMetaData().getColumnTypeName(i);
			
			System.out.println("*");
			System.out.println(rs.getMetaData().getColumnName(i));
			System.out.println(rs.getMetaData().getColumnTypeName(i));
			System.out.println("*");
			
			if(type == "varchar")
			{
				column = merge(new byte[][] {column, new byte[] {(byte) 0xff,(byte) 0xff}, ByteBuffer.allocate(4).putInt(259).array(), new byte[] {0,0}});
			}
			else
			{
				if(type == "int4" || type == "oid")
				{
					column = merge(new byte[][] {column, new byte[] {0,4}});
				}
				else if(type == "text" || type == "name")
				{
					column = merge(new byte[][] {column, ByteBuffer.allocate(2).putShort((short)255).array()});
				}
				else if(type == "int2")
				{
					column = merge(new byte[][] {column, new byte[] {0,2}});
				}
				else if(type == "int8")
				{
					column = merge(new byte[][] {column, new byte[] {0,8}});
				}
				else if(type == "char" || type == "bool")
				{
					column = merge(new byte[][] {column, new byte[] {0,1}});
				}
				
				column = merge(new byte[][] {column, new byte[] {(byte) 0xff,(byte) 0xff,(byte) 0xff,(byte) 0xff},new byte[] {0,0}});
			}
			
			temp = merge(new byte[][] {temp,column});
			length += name.length + 4 + 2 + 4 + 2 + 4 + 2;
		}
		byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
		return merge(new  byte[][] {new byte[] {84}, byteLength, temp});
    }
    
    private byte[] createDataRows(ResultSet rs, short columnCount) throws SQLException
    {
		byte[] dataRow = new byte[0];
		while(rs.next())
		{
			rowCounter++;
			byte[] drId = new byte[] {'D'};
			int length = 4 + 2;
			
			byte[][] columns = new byte[columnCount][];
			for(int i = 0; i < columnCount; i++)
			{
				if(rs.getObject(i+1) == null)
				{
					columns[i] = ByteBuffer.allocate(4).putInt(-1).array();
				}
				else
				{
					String data = rs.getObject(i+1).toString();
					int colLength = data.length();
					length += colLength + 4;
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
			byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
			byte [] byteColumnCount = ByteBuffer.allocate(2).putShort(columnCount).array();
			dataRow = merge(new byte[][] {dataRow, drId,byteLength,byteColumnCount,colData});
		}
		return dataRow;
    }
    
    private byte[] createCommandCompletion(String query)
    {
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
		int length = 4 + byteCommand.length;
		byte[] byteLength = ByteBuffer.allocate(4).putInt(length).array();
		return merge(new byte[][] {ccId,byteLength,byteCommand});
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

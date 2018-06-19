package user;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import aws.dynamodb.PopulateDB;
import services.HeaderTokenizer;
import services.LineTokenizer;
import services.Rule;
import services.Source;
import services.TransactionServices;


public class User {

	public static void main(String[] args) {
		
    	String configFileName = "";
        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (C:\\Users\\suyog\\.aws\\credentials).
         */
        AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("suyog").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. ",e);
        }

        try {
    	   
        		AmazonS3 s3 = new AmazonS3Client(credentials);
        		Region usEast2 = Region.getRegion(Regions.US_EAST_2);
        		s3.setRegion(usEast2);
           
        		/* Reading the properties file*/
        		System.out.println("Enter config file: ");
        		Scanner sc = new Scanner(System.in);
        		configFileName = sc.nextLine();
        		File configFile = new File(configFileName);
        		if(!configFile.exists()) {
        			System.out.println("Config file does not exists");
        			System.exit(0);
        		}
           
        		Source source = new Source();
        		source.readConfigFile(configFile);
           
        		/* Read the rawDB file is required*/
        		File rawDB = null;
        		if(source.rawDatabaseRequired) {
        			System.out.println("Enter rawdatabase file: ");
        			source.dbFile = sc.nextLine();
        			rawDB = source.getTempFile(s3,true);
        			if(!rawDB.exists()) {
        				System.out.println("rawdatabase file does not exists");
        				System.exit(0);
        			}
        		}
           
        		/* List all the files and select one source file to process*/
        		System.out.println("Listing availabel files ..");
        		ObjectListing objectListing = s3.listObjects(new ListObjectsRequest()
                   .withBucketName(source.awsBucketName)
                   .withPrefix(source.awsDirectory+"/"));
        		for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        			System.out.println(" - " + objectSummary.getKey() + "  " +
                                  "(size = " + objectSummary.getSize() + ")");
        		}
           
        		System.out.println("Enter source file name: ");
        		source.sourceFileName = sc.nextLine();
        		File temp = source.getTempFile(s3,false);
          
        		/*Creating the rawDatabase*/
        		PopulateDB db = null;
        		if(source.rawDatabaseRequired) {
        			db = new PopulateDB(source.rawDatabseColumns);
        			db.createTable(source.rawDatabseColumns);
        			db.populateFromCSV(rawDB, source.rawDatabseColumns);
        		}
        		
        		/* Naming the output file*/
        		BufferedReader readBuffer = null;
        		FileOutputStream fos = null;
        		BufferedWriter writeBuffer = null;
        		Date date = new Date() ;
        		SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd") ;
        		source.destinationFileName = dateFormat.format(date)+"_"+ source.sourceFileName;
        		File out = new File(source.destinationFileName);
        		source.outputFile = out;
           
        		/*Start processing source file line by line*/
        		String headerLine;
        		StringBuilder headerBuilder;
        		StringBuilder lineBuilder;
        		int linesProcessed = 0;
        		try {
        			fos = new FileOutputStream(out);
        			writeBuffer = new BufferedWriter(new OutputStreamWriter(fos,"UTF-8"));
        			readBuffer = new BufferedReader(new FileReader(temp));
        			// First line should contain headers
        			headerLine = readBuffer.readLine();
        			
        			// Escaping some junk lines before header line
        			if(source.transactionFile) {
        				while(!headerLine.contains(source.separator) || !headerLine.contains(source.transaction_fileLookup)) {
            				headerLine = readBuffer.readLine();
            			}
        			}
        			else {
        				while(!headerLine.contains(source.separator)) {
            				headerLine = readBuffer.readLine();
            			}
        			}
        			
   			
        			/*Starting from header.*/
        			HeaderTokenizer header = new HeaderTokenizer(headerLine,source); 
        			headerBuilder = header.readHeader();
        			
        			if(source.separator.equals("|")) {
        				System.out.println(headerBuilder.toString().replaceAll("\\|",","));
        				writeBuffer.write(headerBuilder.toString().replaceAll("\\|",","));
        			}
        			else {
        				System.out.println(headerBuilder.toString());
        				writeBuffer.write(headerBuilder.toString());
        			}
        				
   			
        			writeBuffer.newLine();
   			
        			/* Process each line if transaction file*/
        			if(source.transactionFile) {
        				String line;
        				TransactionServices record = new TransactionServices();
        				record.setHeaderToken(header,db);
        				while((line = readBuffer.readLine())!=null) {
        					record.setNewRecord(line);
        					record.processTransaction();
        				}
        				readBuffer.close();
        				record.updateCountsAndWrite(db,writeBuffer,temp);
        				if(source.rawDatabaseRequired) {
            				System.out.println("Deleting Temp table..");
            				db.deleteTable();
        				}	
        			}
        			/* Process each line for log files [Sierra]*/
        			else {
        				String line;
        				LineTokenizer lines = new LineTokenizer();
        				lines.setHeaderToken(header);
        				while((line = readBuffer.readLine())!=null) {
        					lineBuilder = new StringBuilder(line);
        					lines.setNewLine(line, lineBuilder);
        					lines.errorOnLine = false;
        					/* 1.Dropping the columns in @dropcolumn  */
        					lines.dropColumn();
        					/* 2.Changing the date formats of date columns @datecolumn */
        					lines.changeDateFormat();
        					/* 3.For each rule, find the rule name and call specific function  */
   				
        					try {
        						for(Rule rule:source.rules) {
        							switch(rule.ruleName) {
        							case "splitcolumn":
        								lines.splitColumn(rule);
        								break;
   	   						
        							case "splitaddress":
        								lines.splitAddress(rule);
        								break;
   	   						
        							case "add_date":
        								lines.addDate(rule);
        								break;
   	   						
        							case "calculatedays":
        								lines.calculateDays(rule);
        								break;
        								
        							case "selectsingle":
        								lines.selectSingle(rule);
        								break;
        								
        							case "constant":
        								lines.addConstant(rule);
        								break;
        								
        							case "adjustdate":
        								lines.adjustDate(rule);
        								break;
        								
        							case "adjustdateelement":
        								lines.adjustDateElement(rule);
        								break;
        		
        		
        							default:
        								break;
        							}
        						}
        					}catch(StringIndexOutOfBoundsException se) {
        						System.out.println("Error ouccured. Droping line: "+lines.line);
        						continue;
        					}
        					
        					if(lines.errorOnLine)// TO handle blank address (Not a good code, but it was needed)
        						continue;
        					/* 4. Use @strippartial to strip bad value from a record */
        					lines.stripPartial();
        					
        					/* 5. Use @mixedcase global rule */
        					lines.mixedCase();
   				
        					/* 5. Use @droprecord to find bad records and drop the line  */
        					if(lines.dropRecord())
        						continue;
   				
        					/* Write the final record to output file. */
        					if(source.separator.equals("|")) {
        						//System.out.println(lines.lineBuilder.toString().replaceAll("\\|",","));
        						String tempS = lines.lineBuilder.toString().replaceAll("\\|", "");
        						int outColNum = lines.lineBuilder.length() - tempS.length();
        						if(outColNum!=(header.finalHeaderCols.length-1)) {
        							lines.errorList.add(line);
        						}
        							
        						else {
        							/* Replacing normalized bad date value*/
        							String outStr = lines.lineBuilder.toString();
        							if(source.dateColoumn!=null) {
        								String[] dateCols = source.dateColoumn.split(",");
        								for(String colName:dateCols) {
        									if(outStr.contains("NULL"+colName)) {
        										outStr = outStr.replace("NULL"+colName, "");
        									}
        								}
        							}
        							
        							writeBuffer.write(outStr.replaceAll("\\|",","));
        							writeBuffer.newLine();linesProcessed = linesProcessed + 1;
        						}
        							
        					}
        					else {
        						//System.out.println(lines.lineBuilder.toString());
        						String tempS = lines.lineBuilder.toString().replaceAll(",", "");
        						int outColNum = lines.lineBuilder.length() - tempS.length();
        						if(outColNum!=(header.finalHeaderCols.length-1)) {
        							lines.errorList.add(line);
        						}
        							
        						else {
        							/* Replacing normalized bad date value*/
        							String outStr = lines.lineBuilder.toString();
        							if(source.dateColoumn!=null) {
        								String[] dateCols = source.dateColoumn.split(",");
        								for(String colName:dateCols) {
        									if(outStr.contains("NULL"+colName)) {
        										outStr = outStr.replace("NULL"+colName, "");
        									}
        								}
        							}
        							
        							writeBuffer.write(outStr.toString());
        							writeBuffer.newLine();linesProcessed = linesProcessed + 1;
        						}
        							
        					}
        					
        				}
        				System.out.println("Error List: " + lines.errorList.size());
        			}
        		}catch(IOException e) {
        			System.out.println("IOException in reading TempFile..");	
        		}finally {
        			try {
        				readBuffer.close();
						writeBuffer.close();
					} catch (IOException e) {
						
						e.printStackTrace();
					}
        		}
           
        		/*Writing the output file to S3*/
        		if(source.outputFile!=null && source.outputFile.length()>0) {	
        			source.saveOutputFile(s3,linesProcessed);
        			out.delete();
        			source.outputFile.delete();
        		}else {
        			System.out.println("Output file is empty..");
        			out.delete();
        			source.outputFile.delete();
        			System.exit(0);
        		}

           
        }  catch (AmazonServiceException ase) {
        		System.out.println("Caught an AmazonServiceException, which means your request made it "
                   + "to Amazon S3, but was rejected with an error response for some reason.");
        		System.out.println("Error Message:    " + ase.getMessage());
        		System.out.println("HTTP Status Code: " + ase.getStatusCode());
        		System.out.println("AWS Error Code:   " + ase.getErrorCode());
        		System.out.println("Error Type:       " + ase.getErrorType());
        		System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
        		System.out.println("Caught an AmazonClientException, which means the client encountered "
                   + "a serious internal problem while trying to communicate with S3, "
                   + "such as not being able to access the network.");
        		System.out.println("Error Message: " + ace.getMessage());
        }
        System.exit(0);
        
	}
}

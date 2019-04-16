package services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class Source {
	public String sourceName;
	public String subSourceName;
	public String sourceFileName; 
	public String dbFile;
	public String populateCSV; 
	public String destinationFileName;
	public File outputFile;
	public Rule[] rules;
	public String separator;
	public boolean transactionFile;
	public String transaction_fileLookup;//primary key in the transaction file
	public String dropColoumn;
	public String dateColoumn;
	public String currentDateFormat;
	public String convertDateFormat;
	public String awsBucketName;
	public String awsDirectory;
	public String awsRegion;
	public String rawDatabaseDirectory;
	public int loadFile_MaxSize;
	public String dropRecord;
	public String emailColumn;
	public String stripPartial;
	public boolean rawDatabaseRequired;
	public String rawDatabseColumns; //String denoting column names from rawDB file 
	public String recordFormat;
	public String mixedCase;
	public String mauticUserName;
	public String mauticPassword;
	public String mauticApi;
	public String mauticfields;
	public String segmentId;
	public String mobileColumn;
	public String countryCode;

	public Source() {
		sourceName = "";
		subSourceName = "";
		sourceFileName = ""; 
		populateCSV = ""; 
		destinationFileName = "";
		outputFile = null;
		rules = null;
		transactionFile = false;
		transaction_fileLookup = "";
		dropColoumn = "";
		convertDateFormat = "";
		awsBucketName = "";
		awsDirectory = "";
		loadFile_MaxSize = -1;
		dropRecord = "";
		stripPartial = "";
	}
	
	public File getTempFile(AmazonS3 s3,boolean isDBFile)throws AmazonServiceException{
		FileOutputStream fos;
		File tempFile = null;
		      
		System.out.println("Accessing file: "+ (isDBFile?this.dbFile:this.sourceFileName));
		try {
			tempFile = File.createTempFile((isDBFile?this.dbFile:this.sourceFileName),"");
			tempFile.deleteOnExit();
			S3Object o = null;
			if(isDBFile)
				o = s3.getObject(this.awsBucketName, this.rawDatabaseDirectory+"/"+this.dbFile);
			else
				o = s3.getObject(this.awsBucketName, this.awsDirectory+"/"+this.sourceFileName);
			
			S3ObjectInputStream s3is = o.getObjectContent();
			
			fos = new FileOutputStream(tempFile);
		
			byte[] read_buf = new byte[1024];
			int read_len = 0;
			while ((read_len = s3is.read(read_buf)) > 0) {
				fos.write(read_buf, 0, read_len);
				}
			System.out.println("Read successfully..");
			s3is.close();
	        fos.close();
			}catch (FileNotFoundException e) {
				System.err.println(e.getMessage());;
			}catch (AmazonServiceException e) {
				System.out.println("File does not exist on Bucket..");
	            System.err.println(e.getErrorMessage());
	            System.exit(1);
	        } catch (IOException e) {
	        	System.err.println(e.getMessage());
			}
		return tempFile;
	}
	
	public void saveOutputFile(AmazonS3 s3,int linesProcessed){
		
		if(this.loadFile_MaxSize!=-1 && this.outputFile.length()> (this.loadFile_MaxSize*1000000)) {
			List<File> list;
			list = splitUploadFile(linesProcessed);
			if(list!=null) {
				TransferManager tm = new TransferManager(new ProfileCredentialsProvider("suyog"));
				for(File out:list) {
					System.out.println("Uploading file: "+ out.getName() +" to S3");
					
					Upload upload = tm.upload(
							this.awsBucketName, this.awsDirectory+"/"+out.getName(), out);
					try {
			        	// Or you can block and wait for the upload to finish
			        	upload.waitForCompletion();
			        	System.out.println("Upload complete for file "+out.getName());
			        } catch (AmazonClientException amazonClientException) {
			        	System.out.println("Unable to upload file "+out.getName()+" , upload was aborted.");
			        	amazonClientException.printStackTrace();
			        } catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					out.delete();
				}
			}
		}
		else {
			Date date = new Date() ;
			SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd") ;
			this.destinationFileName = dateFormat.format(date)+"_"+ this.sourceFileName;
			
			System.out.println("Uploading file: "+ this.destinationFileName +" to S3");
			TransferManager tm = new TransferManager(new ProfileCredentialsProvider("suyog"));
			Upload upload = tm.upload(
					this.awsBucketName, this.awsDirectory+"/"+this.destinationFileName, this.outputFile);
			try {
	        	// Or you can block and wait for the upload to finish
	        	upload.waitForCompletion();
	        	System.out.println("Upload complete.");
	        } catch (AmazonClientException amazonClientException) {
	        	System.out.println("Unable to upload file, upload was aborted.");
	        	amazonClientException.printStackTrace();
	        } catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
		
       //s3.putObject(new PutObjectRequest(bucketName, this.destinationFileName, this.outputFile));
       System.out.println("Uploading done.. ");
	}
	
	public List<File> splitUploadFile(int linesProcessed) {
		BufferedReader readBuffer = null;BufferedWriter writeBuffer = null;
	    FileOutputStream fos = null;
	    List<File> listOfFiles = new ArrayList<File>();
	    Date date = new Date() ;
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd") ;
		String outFile	 = dateFormat.format(date)+"_"+ this.sourceFileName;
			
		long numberOfSplit = (this.outputFile.length()/(this.loadFile_MaxSize * 1000000))+1;
		long linesPerSplit = linesProcessed / numberOfSplit;
		try {
			readBuffer = new BufferedReader(new FileReader(this.outputFile));
		
		String headerLine = readBuffer.readLine();
		String line;
		
		for(int i=1;i<=numberOfSplit;i++) {
			File out = new File(i+"_"+outFile);
			fos = new FileOutputStream(out);
			writeBuffer = new BufferedWriter(new OutputStreamWriter(fos,"UTF-8"));
			writeBuffer.write(headerLine);writeBuffer.newLine();
			long currentLine = 1;
			while(currentLine<=linesPerSplit && (line = readBuffer.readLine())!=null) {
				writeBuffer.write(line);writeBuffer.newLine();
				currentLine++;
			}
			if(i==numberOfSplit && currentLine>=linesPerSplit) {
				while((line = readBuffer.readLine())!=null) {
					writeBuffer.write(line);writeBuffer.newLine();
				}
			}
			writeBuffer.close();
			listOfFiles.add(out);
		}
		readBuffer.close();
	
		
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return listOfFiles;
	}
	
	public void readConfigFile(File config) {
		Properties properties = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream(config);
			properties.load(input);
		} catch (IOException e) {// TODO Auto-generated catch block
			System.out.println("Error in reading config file.");
			e.printStackTrace();
		}
		
		String transactionFile = properties.getProperty("transaction_file");
		if(transactionFile!=null) {
			if(transactionFile.equalsIgnoreCase("true")) {
				this.setTransactionFile(true);
				this.setTransaction_fileLookup(properties.getProperty("transaction_filelookup"));
				this.setRecordFormat(properties.getProperty("recordformat"));
			}
			else
				this.setTransactionFile(false);
		}
		
		String rawdatabase = properties.getProperty("rawdatabaserequired");
		if(rawdatabase!=null) {
			if(rawdatabase.equalsIgnoreCase("true")) {
				this.setRawDatabaseRequired(true);
				this.setRawDatabaseColumns(properties.getProperty("rawdatabasecolumn"));
			}
			else
				this.setRawDatabaseRequired(false);
		}
		
		this.setDropColoumn(properties.getProperty("dropcolumn"));
		this.setEmailColumn(properties.getProperty("emailcolumn"));
		this.setDateColoumn(properties.getProperty("datecolumn"));
		this.setCurrentDateFormat(properties.getProperty("currentdates"));
		this.setConvertDateFormat(properties.getProperty("convertdates"));
		this.setAwsBucketName(properties.getProperty("awsbucketname"));
		this.setAwsDirectory(properties.getProperty("awsdirectory"));
		this.setAwsRegion(properties.getProperty("awsregion"));
		if(properties.getProperty("loadfile_maxsize")!=null)
			this.setLoadFile_MaxSize(Integer.parseInt(properties.getProperty("loadfile_maxsize")));
		this.setDropRecord(properties.getProperty("droprecord"));
		this.setSeparator(properties.getProperty("separator"));
		this.setStripPartial(properties.getProperty("strippartial"));
		if(properties.getProperty("rule")!=null)
			this.setRules(properties.getProperty("rule"));
		this.setMixedCase(properties.getProperty("mixedcase"));
		this.setDBDirectory(properties.getProperty("rawdatabasedirectory"));
		this.setMauticApi(properties.getProperty("mauticapi"));
		this.setMauticPassword(properties.getProperty("mauticpassword"));
		this.setMauticUserName(properties.getProperty("mauticusername"));
		this.setMauticfields(properties.getProperty("mauticfields"));
		this.setSegmentId(properties.getProperty("segmentId", "-100"));
		this.setMobileColumn(properties.getProperty("mobilecolumn"));
		this.setCountryCode(properties.getProperty("countrycode"));
	}
	
	public void setAwsRegion(String awsRegion) {
		this.awsRegion = awsRegion;
	}
	
	public String getAwsRegion() {
		return this.awsRegion;
	}
	
	public void setMobileColumn(String mobileCol) {
		this.mobileColumn = mobileCol;
	}
	
	public String getMobileColumn() {
		return this.mobileColumn;
	}
	
	public void setCountryCode(String countryCode) {
		this.countryCode = countryCode;
	}
	
	public String getCountryCode() {
		return this.countryCode;
	}
	
	public String getSegmentId() {
		return segmentId;
	}

	public void setSegmentId(String segmentId) {
		this.segmentId = segmentId;
	}

	public String getMauticfields() {
		return mauticfields;
	}

	public void setMauticfields(String mauticfields) {
		this.mauticfields = mauticfields;
	}

	public String getMauticUserName() {
		return mauticUserName;
	}

	public void setMauticUserName(String mauticUserName) {
		this.mauticUserName = mauticUserName;
	}

	public String getMauticPassword() {
		return mauticPassword;
	}

	public void setMauticPassword(String mauticPassword) {
		this.mauticPassword = mauticPassword;
	}

	public String getMauticApi() {
		return mauticApi;
	}

	public void setMauticApi(String mauticApi) {
		this.mauticApi = mauticApi;
	}

	public String getDBDirectory() {
		return this.rawDatabaseDirectory;
	}
	
	public void setDBDirectory(String rawDatabaseDirectory) {
		this.rawDatabaseDirectory = rawDatabaseDirectory;
	}
	
	public String getCurrentDateFormat() {
		return currentDateFormat;
	}

	public void setCurrentDateFormat(String currentDateFormat) {
		this.currentDateFormat = currentDateFormat;
	}

	public String getMixedCase() {
		return mixedCase;
	}

	public void setMixedCase(String mixedCase) {
		this.mixedCase = mixedCase;
	}

	private void setRecordFormat(String recordFormat) {
		this.recordFormat = recordFormat;	
	}

	public String getSourceName() {
		return sourceName;
	}

	public void setSourceName(String sourceName) {
		this.sourceName = sourceName;
	}

	public String getSubSourceName() {
		return subSourceName;
	}

	public void setSubSourceName(String subSourceName) {
		this.subSourceName = subSourceName;
	}
	
	public void setTransactionLookup(String transactionLookup) {
		this.transaction_fileLookup = transactionLookup;
	}
	
	public String getTransactionLookup() {
		return transaction_fileLookup;
	}
	
	public String getSourceFileName() {
		return sourceFileName;
	}
	
	public boolean getRawDatabaseRequired() {
		return rawDatabaseRequired;
	}

	public void setRawDatabaseRequired(Boolean rawDatabaseRequired) {
		this.rawDatabaseRequired = rawDatabaseRequired;
	}
	
	public void setRawDatabaseColumns(String rawDatabseColumns) {
		this.rawDatabseColumns = rawDatabseColumns;
	}

	public String getRawDatabaseColumns() {
		return rawDatabseColumns;
	}
	
	public void setSourceFileName(String sourceFileName) {
		this.sourceFileName = sourceFileName;
	}

	public String getPopulateCSV() {
		return populateCSV;
	}

	public void setPopulateCSV(String populateCSV) {
		this.populateCSV = populateCSV;
	}

	public String getDestinationFileName() {
		return destinationFileName;
	}

	public void setDestinationFileName(String destinationFileName) {
		this.destinationFileName = destinationFileName;
	}

	public File getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(File outputFile) {
		this.outputFile = outputFile;
	}

	public Rule[] getRules() {
		return rules;
	}

	public void setRules(String ruleString) {
		String[] ruleStrings = ruleString.split("&&");
		Rule[] rules = new Rule[ruleStrings.length];
		int i=0;
		for(String str:ruleStrings) {
			Rule r = new Rule(str);
			rules[i] = r;i++;
		}
		
		
		this.rules = rules;
	}

	public boolean isTransactionFile() {
		return transactionFile;
	}

	public void setTransactionFile(boolean transactionFile) {
		this.transactionFile = transactionFile;
	}

	public String getTransaction_fileLookup() {
		return transaction_fileLookup;
	}

	public void setTransaction_fileLookup(String transaction_fileLookup) {
		this.transaction_fileLookup = transaction_fileLookup;
	}

	public String getDropColoumn() {
		return dropColoumn;
	}

	public void setEmailColumn(String emailColumn) {
		this.emailColumn = emailColumn;
	}
	
	public String getEmailColumn() {
		return emailColumn;
	}
	
	public void setDropColoumn(String dropColoumn) {
		this.dropColoumn = dropColoumn;
	}

	public String getDateColoumn() {
		return dateColoumn;
	}

	public void setDateColoumn(String dateColoumn) {
		this.dateColoumn = dateColoumn;
	}

	public String getConvertDateFormat() {
		return convertDateFormat;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}
	
	public String getSeparator() {
		return separator;
	}
	
	public void setConvertDateFormat(String convertDateFormat) {
		this.convertDateFormat = convertDateFormat;
	}

	public String getAwsBucketName() {
		return awsBucketName;
	}

	public void setAwsBucketName(String awsBucketName) {
		this.awsBucketName = awsBucketName;
	}

	public String getAwsDirectory() {
		return awsDirectory;
	}

	public void setAwsDirectory(String awsDirectory) {
		this.awsDirectory = awsDirectory;
	}

	public int getLoadFile_MaxSize() {
		return loadFile_MaxSize;
	}

	public void setLoadFile_MaxSize(int loadFile_MaxSize) {
		this.loadFile_MaxSize = loadFile_MaxSize;
	}

	public String getDropRecord() {
		return dropRecord;
	}

	public void setDropRecord(String dropRecord) {
		this.dropRecord = dropRecord;
	}

	public String getStripPartial() {
		return stripPartial;
	}

	public void setStripPartial(String stripPartial) {
		this.stripPartial = stripPartial;
	}
}

package services;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import aws.dynamodb.PopulateDB;

public class TransactionServices {
	public String line;
	public StringBuilder lineBuilder;
	public String[] cols;
	public HeaderTokenizer headerToken;
	public ArrayList<String> errorList;
	public PopulateDB db;
	public boolean isRecordProcessed;
	public String currentRecordKey;
	public HashMap<String,HashMap<String,Transaction>> recordCountMap;
	
	public TransactionServices(){
	
	}
	
	public void setHeaderToken(HeaderTokenizer headerToken,PopulateDB db) {
		this.recordCountMap = new HashMap<String,HashMap<String,Transaction>>();
		this.headerToken = headerToken;
		this.errorList = new ArrayList<String>();
		this.db = db;
	}
	
	public void setNewRecord(String line) {
		this.line = line;
		this.lineBuilder = new StringBuilder(line);
		if(headerToken.source.separator.equals(",")) {
			this.cols = this.line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)",-1);
			for(int i=0;i<cols.length;i++) {
				String st = cols[i].replaceAll("\"", "");
				cols[i] = st;
			}
		}
			
	
		if(headerToken.source.separator.equals("|")) {
			this.cols = this.line.split("\\|",-1);	
			for(int i=0;i<cols.length;i++) {
				String st = cols[i].replaceAll("\"", "");
				cols[i] = st;
			}
		}
			
	}
	
	class Transaction{
		public String format;
		public String checkoutDate;
		public int count;
		
		public Transaction() {
			this.count =0;
			this.checkoutDate = null;
		}
		
		public Transaction(String format) {
			this.format = format;
			this.count =0;
			this.checkoutDate = null;
		}
		public int getCount() {
			return count;
		}
		public void setCount(int count) {
			this.count = count;
		}
		public String getFormat() {
			return format;
		}
		public void setFormat(String format) {
			this.format = format;
		}
		public String getCheckoutDate() {
			return checkoutDate;
		}
		public void setCheckoutDate(String checkoutDate) {
			this.checkoutDate = checkoutDate;
		}
	}
	
	public void processTransaction() {
		String recordFormat = this.headerToken.source.recordFormat;
		/*Reading the col names and format names from properties file*/
		String[] formatParts = recordFormat.split("\\|");
		String primaryKeyCol = formatParts[0];
		String formatCol = formatParts[1];
		String formatString= formatParts[2];
		String[] formatNames = null;
		if(!formatParts[2].equals("NULL")) {
			formatNames = formatString.split(";");
		}
		String countCol = formatParts[3];
		String checkOutDateCol = formatParts[4];
		
		/*Getting the index of cols*/
		int primaryKeyColInd=-1,formatColInd=-1,countColInd=-1,checkOutDateColInd =-1;
		for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
			String colName = this.headerToken.originalHeaderCols[i];
			if(colName.equals(primaryKeyCol)) {
				primaryKeyColInd = i;
			}
			if(colName.equals(formatCol)) {
				formatColInd = i;
			}
			if(colName.equals(countCol)) {
				countColInd = i;
			}
			if(colName.equals(checkOutDateCol)) {
				checkOutDateColInd = i;
			}
		}
		
		/*Getting the actual values of the record*/
		String primaryKeyValue = this.cols[primaryKeyColInd];
		String formatValue = null;
		if(formatColInd!=-1)
			formatValue = this.cols[formatColInd];
		
		String countValue = null;
		if(countColInd!=-1)
			countValue = this.cols[countColInd];
		
		String checkOutDate = null;
		if(checkOutDateColInd!=-1) 
			checkOutDate = this.cols[checkOutDateColInd];
		
		
		HashMap<String, Transaction> transactionMap = null;
		Transaction transaction = null;
		try {
			if(this.recordCountMap.containsKey(primaryKeyValue)) {
				transactionMap = this.recordCountMap.get(primaryKeyValue);
				if(formatValue!=null) {
					if(formatNames!=null) {
						for(String formatType:formatNames) {
							/* If Record is of one format type e.g ebook or audiobook */
							if(formatType.equals(formatValue)) {
							/*  If there is a column containing count, then add to the current count.e.g: Hoopla */
								if(countValue!=null) {
									int currCount = 0;
									if(transactionMap.containsKey(formatValue)) {
										Transaction tran = transactionMap.get(formatValue);
										int count = tran.getCount() + Integer.parseInt(countValue);
										tran.setCount(count);
										transactionMap.put(formatValue, tran);
									}
									else {
										Transaction tran = new Transaction();
										tran.setFormat(formatValue); tran.setCount(Integer.parseInt(countValue));
										if(checkOutDate!=null) {
											String date = this.convertDateFormat(checkOutDate);
											tran.setCheckoutDate(date);
										}else {
											SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
											Calendar c = Calendar.getInstance();   // this takes current date
										    c.set(Calendar.DAY_OF_MONTH, 1);
										    Date d = c.getTime();
										    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
										}
										transactionMap.put(formatValue,tran);
									}
									/* Else increment the current count.e.g: Overdrive */
								}else {
									if(transactionMap.containsKey(formatValue)) {
										Transaction tran = transactionMap.get(formatValue);
										int count = tran.getCount() + 1;
										tran.setCount(count);
										transactionMap.put(formatValue, tran);
									}else {
										Transaction tran = new Transaction();
										tran.setFormat(formatValue); tran.setCount(1);
										if(checkOutDate!=null) {
											String date = this.convertDateFormat(checkOutDate);
											tran.setCheckoutDate(date);
										}else {
											SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
											Calendar c = Calendar.getInstance();   // this takes current date
										    c.set(Calendar.DAY_OF_MONTH, 1);
										    Date d = c.getTime();
										    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
										}
										transactionMap.put(formatValue,tran);
									}
								}
							}
						}
					}
				}
				/* for Zinio, there is not format type e.g no ebooks or audiobooks */
				else {
					transactionMap = this.recordCountMap.get(primaryKeyValue);
					Transaction tran = transactionMap.get(null);
					/* If there is a column containing count, then add to the current count.e.g: Hoopla */
					if(countValue!=null) {
						int count = tran.getCount() + Integer.parseInt(countValue);
						tran.setCount(count);
					}
					/* Else increment the current count.e.g: Overdrive */
					else {
						int count = tran.getCount() + 1;
						tran.setCount(count);
					}
					if(checkOutDate!=null) {
						String date = this.convertDateFormat(checkOutDate);
						tran.setCheckoutDate(date);
					}else {
						SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
						Calendar c = Calendar.getInstance();   // this takes current date
					    c.set(Calendar.DAY_OF_MONTH, 1);
					    Date d = c.getTime();
					    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
					}
				}
				
				
				this.recordCountMap.put(primaryKeyValue,transactionMap);

			}
			
			else {
				transactionMap = new HashMap<String,Transaction>();
				/* for Hoopla and Overdrive, there are format types e.g ebooks or audiobooks */ 
				if(formatValue!=null) {
					if(formatNames!=null) {
						for(String formatType:formatNames) {
							/* If Record is of one format type e.g ebook or audiobook */
							if(formatType.equals(formatValue)) {
								/* If there is a column containing count, then add to the current count.e.g: Hoopla */
								if(countValue!=null) {
									Transaction tran = new Transaction();
									tran.setFormat(formatValue); tran.setCount(Integer.parseInt(countValue));
									if(checkOutDate!=null) {
										String date = this.convertDateFormat(checkOutDate);
										tran.setCheckoutDate(date);
									}else {
										SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
										Calendar c = Calendar.getInstance();   // this takes current date
									    c.set(Calendar.DAY_OF_MONTH, 1);
									    Date d = c.getTime();
									    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
									}
									transactionMap.put(formatValue, tran);
								}
								/* Else increment the current count.e.g: Overdrive */
								else {
									Transaction tran = new Transaction();
									tran.setFormat(formatValue); tran.setCount(1);
									if(checkOutDate!=null) {
										String date = this.convertDateFormat(checkOutDate);
										tran.setCheckoutDate(date);
									}else {
										SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
										Calendar c = Calendar.getInstance();   // this takes current date
									    c.set(Calendar.DAY_OF_MONTH, 1);
									    Date d = c.getTime();
									    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
									}
									transactionMap.put(formatValue, tran);
								}
							}
						}
					}
				}
				/* for Zinio, there is not format type e.g no ebooks or audiobooks */
				else {
					/* If there is a column containing count, then add to the current count.e.g: Hoopla */
						if(countValue!=null) {
							Transaction tran = new Transaction();
							tran.setCount(Integer.parseInt(countValue));
							if(checkOutDate!=null) {
								String date = this.convertDateFormat(checkOutDate);
								tran.setCheckoutDate(date);
							}else {
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
								Calendar c = Calendar.getInstance();   // this takes current date
							    c.set(Calendar.DAY_OF_MONTH, 1);
							    Date d = c.getTime();
							    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
							}
							transactionMap.put(null, tran);
						}
						/* Else set the count to 1.e.g: Overdrive */
						else {
							Transaction tran = new Transaction();
							tran.setCount(1);
							if(checkOutDate!=null) {
								String date = this.convertDateFormat(checkOutDate);
								tran.setCheckoutDate(date);
							}else {
								SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
								Calendar c = Calendar.getInstance();   // this takes current date
							    c.set(Calendar.DAY_OF_MONTH, 1);
							    Date d = c.getTime();
							    tran.setCheckoutDate(dateFormat.format(d)); // first date of current month
							}
							transactionMap.put(null, tran);
						}
					
				}
				this.recordCountMap.put(primaryKeyValue,transactionMap);
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE] on the line: "+line);
		}catch(NullPointerException ne) {
			System.out.println("[NE] on the line: "+line);
		}
		
		
	}

	public void updateCountsAndWrite(PopulateDB db, BufferedWriter writeBuffer, File temp) {
	/* Go over all the records to count total checkouts for each primary key*/
	/* Read from start every line, drop the columns, ad the new and remove the entry from Map once read*/	
		 BufferedReader readerBuffer = null;
		 String line = "";StringBuilder record;
		 String headerLine="";
		 try {
			 	readerBuffer = new BufferedReader(new FileReader(temp));
			 	// First line should contain headers
			 	headerLine = readerBuffer.readLine();
			 	while(!headerLine.contains(this.headerToken.source.separator) || !headerLine.contains(this.headerToken.source.transaction_fileLookup)) {
    				headerLine = readerBuffer.readLine();
    			}
	
			 	while((line = readerBuffer.readLine())!=null) {
			 		this.setNewRecord(line);
			 		String primaryKey = this.headerToken.source.transaction_fileLookup;
			 		int primaryKeyColIndex = 0;
			 		for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
						if(primaryKey.equals(this.headerToken.originalHeaderCols[i])) {
							primaryKeyColIndex	= i;break;
						}		
					}
			 		this.currentRecordKey = this.cols[primaryKeyColIndex];
			 		if(!this.recordCountMap.containsKey(currentRecordKey))
			 			continue;
			 		
			 		this.dropColumn(this.lineBuilder);
			 		try {
			 				for(Rule rule:this.headerToken.source.rules) {
			 					switch(rule.ruleName) {
  						
			 						case "findfromdb":
			 							this.findFromDB(rule,this.lineBuilder,line);
			 							break;
  						
			 						case "total&date":
			 							this.totalAndDate(rule,this.lineBuilder,line);
			 							break;
  						
			 						case "add_date":
			 							this.addDate(rule,this.lineBuilder,line);
			 							break;
			 							
			 						case "splitcolumn":
			 							this.splitColumn(rule,this.lineBuilder,line);
			 							break;
			 							
			 						case "adjustdate":
			 							this.adjustDate(rule,this.lineBuilder,line);
			 							break;
			 						default:
			 							break;
							}
						}
					}catch(StringIndexOutOfBoundsException se) {
						System.out.println("Error ouccured. Droping line: "+line);
						continue;
					}
			 		
			 		if(!this.dropRecord()) {
			 			//System.out.println(this.lineBuilder.toString());
			 			writeBuffer.write(this.lineBuilder.toString());writeBuffer.newLine();
			 		}
		
			 		this.recordCountMap.remove(currentRecordKey);
			 	}
 			
		 }catch(IOException e) {
 			System.out.println("IOException in reading TempFile..");	
 		}finally {
 			try {
 				readerBuffer.close();
					writeBuffer.close();
				} catch (IOException e) {	
					e.printStackTrace();
				}
 		}
	}
	
	private void adjustDate(Rule rule, StringBuilder lineBuilder, String line) {
		if(rule==null || rule.adjustDate==null)
			return;
		
		String[] arr = rule.adjustDate.split(",");
		boolean add = false,minus = false;
		if(arr[0].equals("add")) {
			add = true;
		}else if(arr[0].equals("minus")){
			minus = true;
		}
		
		int days = Integer.parseInt(arr[1]);
		String dateValueOld = null,dateValueNew=null;
		String dateValueOldCol = rule.sourceColoumn;
		String[] dest = rule.destinationColumn;
		String dateValueNewCol = dest[0];
		
		for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
			if(dateValueOldCol.equals(headerToken.originalHeaderCols[i])) {
				int dateColIndex = i;
				dateValueOld = this.cols[dateColIndex];
				break;
			}
		}

		
	}

	public void splitColumn(Rule rule, StringBuilder lineBuilder, String line) {
		try {
			int sourceColIndex = -1;
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(rule.sourceColoumn.equals(headerToken.originalHeaderCols[i])) {
					sourceColIndex = i; 
					break;
				}
			}
			
			String sourceColValue = cols[sourceColIndex];
			String[] splitValue = sourceColValue.split(rule.splitSeparator);
			String finalSplitString = "";
			
			for(int k:rule.spliElementsToKeep) {
				k--;
				if(rule.mixedCase) {
					if(splitValue.length>=k+1) {
						String value = splitValue[k].trim();
						if(rule.stripMiddleInitial) {
							String[] middleInitial = value.split(" ");
							value = middleInitial[0];
						}
						String capitalizedValue = value.substring(0, 1).toUpperCase() + value.substring(1).toLowerCase();
						finalSplitString = finalSplitString + capitalizedValue + this.headerToken.source.separator;
					}else {
						finalSplitString = finalSplitString + this.headerToken.source.separator;
					}
					
				}else {
					if(splitValue.length>=k+1) {
						String value = splitValue[k].trim();
						if(rule.stripMiddleInitial) {
							String[] middleInitial = value.split(" ");
							value = middleInitial[0];
						}
						finalSplitString = finalSplitString + value + this.headerToken.source.separator;	
					}else {
						finalSplitString = finalSplitString + this.headerToken.source.separator;
					}
						
				}
			}
			int startInd = lineBuilder.indexOf(sourceColValue);
			lineBuilder.replace(startInd, startInd + sourceColValue.length() + 1, finalSplitString);

		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in splitcolumn: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in splitcolumn: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in splitcolumn: "+ line);
		}
	}
	
	public boolean dropRecord() {
		
	try {
		if(headerToken.source.emailColumn!=null) {
			String[] col = this.lineBuilder.toString().split(",");
			
			for(int i=0;i<this.headerToken.finalHeaderCols.length;i++) {
				if(headerToken.source.emailColumn.equals(headerToken.finalHeaderCols[i])) {
					
					if(col[i].equals("") || col[i].equals(" "))
						return true;
					break;
				}
			}
		}
	}catch(ArrayIndexOutOfBoundsException ae) {
		System.out.println("[AE] While dropping record: "+line);
	}catch(NullPointerException ne) {
		System.out.println("[NE] While dropping record: "+line);
	}
		return false;
	}
	
	
	public void findFromDB(Rule rule, StringBuilder record,String line) {
		String primaryKey = this.headerToken.source.transaction_fileLookup;
		int primaryKeyColIndex = -1;
		String primaryKeyValue;
		for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
			if(primaryKey.equals(this.headerToken.originalHeaderCols[i])) {
				primaryKeyColIndex	= i;break;
			}	
		}
		
		String[] col=null;
		if(headerToken.source.separator.equals(",")) {
			col = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)",-1);
			for(int i=0;i<col.length;i++) {
				String st = col[i].replaceAll("\"", "");
				col[i] = st;
			}
		}

		if(headerToken.source.separator.equals("|")) {
			col = line.split("\\|",-1);	
			for(int i=0;i<col.length;i++) {
				String st = col[i].replaceAll("\"", "");
				col[i] = st;
			}
		}
		String finalString = "";
		try {
			primaryKeyValue = col[primaryKeyColIndex];
			String[] findCols = rule.findFromDB.split(",");
			String[] findColsValue = db.getFromDB(primaryKeyValue, findCols);
			if(rule.sourceColoumn.equals("NULL") && findColsValue!=null) {
				for(int i=0;i<rule.destinationColumn.length;i++) {
					if(i==rule.destinationColumn.length-1)
						finalString = finalString + findColsValue[i];
					else
						finalString = finalString + findColsValue[i] + headerToken.source.separator;
				}
			}else if(findColsValue==null) {
				for(int i=0;i<rule.destinationColumn.length;i++) {
					if(i==rule.destinationColumn.length-1)
						finalString = finalString + "";
					else
						finalString = finalString + "" + headerToken.source.separator;
				} 
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]DB did not find the record: "+line);
			for(int i=0;i<rule.destinationColumn.length;i++) {
				if(i==rule.destinationColumn.length-1)
					finalString = finalString + "";
				else
					finalString = finalString + "" + headerToken.source.separator;
			}
		}catch(NullPointerException ne) {
			System.out.println("[NE]DB did not find the record: "+line);
			for(int i=0;i<rule.destinationColumn.length;i++) {
				if(i==rule.destinationColumn.length-1)
					finalString = finalString + "";
				else
					finalString = finalString + "" + headerToken.source.separator;
			}
		}
		record.append(headerToken.source.separator + finalString);	
		
	}
	
	public void totalAndDate(Rule rule, StringBuilder record,String line) {
		String primaryKey = this.headerToken.source.transaction_fileLookup;
		int primaryKeyColIndex = 0;
		String primaryKeyValue;
		try {
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(primaryKey.equals(this.headerToken.originalHeaderCols[i])) {
					primaryKeyColIndex	= i;break;
				}	
			}
			
			/*String[] col=null;
			if(headerToken.source.separator.equals(",")) {
				col = line.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)",-1);
				for(int i=0;i<col.length;i++) {
					String st = col[i].replaceAll("\"", "");
					col[i] = st;
				}
			}

			if(headerToken.source.separator.equals("|")) {
				col = line.split("\\|",-1);	
				for(int i=0;i<col.length;i++) {
					String st = col[i].replaceAll("\"", "");
					col[i] = st;
				}
			}*/
			
			primaryKeyValue = this.cols[primaryKeyColIndex];
			if(this.recordCountMap.containsKey(primaryKeyValue)) {
				HashMap<String, Transaction> countMap = this.recordCountMap.get(primaryKeyValue);
				int totalCount = 0;String date = "";
				if(!rule.transaction_format.equals("NULL")) {
					if(countMap.containsKey(rule.transaction_format)) {
						Transaction tran = countMap.get(rule.transaction_format);
						totalCount = tran.getCount();
						date = tran.getCheckoutDate();
					}else {
						totalCount = 0;
						date = "";
					}
				}else {
					for(Map.Entry<String, Transaction> rec:countMap.entrySet()) {
						totalCount = totalCount + rec.getValue().count;
						if(rec.getValue().checkoutDate!=null) {
							date =rec.getValue().checkoutDate; 
						}else {
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							Calendar c = Calendar.getInstance();   // this takes current date
						    c.set(Calendar.DAY_OF_MONTH, 1);
						    Date d = c.getTime();
						    date  = dateFormat.format(d); // first date of current month
						}
					}
					
					
					
					// Use below code for first date of month for not format last checkout date
					
					/*// Temporary: last checkout date = firstDate of the month
					SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
					Calendar c = Calendar.getInstance();   // this takes current date
				    c.set(Calendar.DAY_OF_MONTH, 1);
				    Date d = c.getTime();
				    date  = dateFormat.format(d); // first date of current month
*/				}
				record.append(this.headerToken.source.separator + totalCount +this.headerToken.source.separator + date);
				//this.recordCountMap.remove(primaryKeyValue);
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE] on the line: "+line);
		}catch(NullPointerException ne) {
			System.out.println("[NE] on the line: "+line);
		}		
	}
	
	public void addDate(Rule rule, StringBuilder record,String line) {
		if(rule.sourceColoumn.equals("NULL")) {
			Date date = new Date();
			if(this.headerToken.source.convertDateFormat!=null) {
				SimpleDateFormat f = new SimpleDateFormat(this.headerToken.source.convertDateFormat);
				record.append(headerToken.source.separator +f.format(date));
			}
			else {
				SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
				record.append(headerToken.source.separator +f.format(date));
			}	
		}	
		
	}
	
	public String convertDateFormat(String date) {
		String convertedDate = "";
		String currDateFormat = this.headerToken.source.currentDateFormat;
		DateFormat formatter1 = null;
		
		
		
		if(date.length()>9) {
			formatter1 = new SimpleDateFormat(currDateFormat);
			
		}else {
			if(currDateFormat.equals("MM-dd-yyyy")) { 
				formatter1 = new SimpleDateFormat("MM-dd-yy");
				
			}	
			if(currDateFormat.equals("MM/dd/yyyy")) {
				String[] mmdd = date.split("/");
				if(mmdd[0].length()==1 && mmdd[1].length()==1)
					formatter1 = new SimpleDateFormat("M/d/yyyy");
				if(mmdd[0].length()==2 && mmdd[1].length()==1)
					formatter1 = new SimpleDateFormat("MM/d/yyyy");
				if(mmdd[0].length()==1 && mmdd[1].length()==2)
					formatter1 = new SimpleDateFormat("M/dd/yyyy");
			}
				
		}
		
		Date oldCreateDate;
		try {
			oldCreateDate = (Date) formatter1.parse(date);
		
			SimpleDateFormat newCreateDate = new SimpleDateFormat("yyyy-MM-dd");
			convertedDate = newCreateDate.format(oldCreateDate);
		}catch (ParseException e) {
			System.out.println(date +" parsing error on line "+ line);
			}
		return convertedDate;
	}
	
	public void dropColumn(StringBuilder record) {
		if(headerToken.source.dropColoumn==null || headerToken.source.dropColoumn.equals(""))
			return;
			
		try {
			String[] dropCols = headerToken.source.dropColoumn.split(",");
			int[] dropColsIndex = new int[dropCols.length];
			int k =0;
			for(String dropCol: dropCols) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(dropCol.equals(headerToken.originalHeaderCols[i])) {
						dropColsIndex[k] = i; k++;
						break;
					}
				}
			}
			
			
			
			
			TreeMap<Integer,String> map = new TreeMap<Integer,String>();
			int colIndex = 0;
			
			for(String colValue:this.cols) {
				map.put(colIndex, colValue);
				colIndex++;
			}
			
			for(int dropColIndex:dropColsIndex) {
				map.remove(dropColIndex);
			}
			
			StringBuilder afterDroppedCols = new StringBuilder("");
			int mapLen = 0;
			for(Map.Entry<Integer, String> entry:map.entrySet()) {
				if(mapLen==map.size()-1)
					afterDroppedCols.append(entry.getValue());
				else{
					afterDroppedCols.append(entry.getValue()+headerToken.source.separator);
					mapLen++;
				}
				
			}
			int len = record.length();
			record.replace(0,len, afterDroppedCols.toString());
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in droppin columns: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in droppin columns: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in droppin columns: "+ line);
		}
		
	}
}

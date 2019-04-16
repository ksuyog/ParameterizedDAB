package services;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.lang3.*;

public class LineTokenizer {

	public String line;
	public StringBuilder lineBuilder;
	public String[] cols;
	public HeaderTokenizer headerToken;
	public ArrayList<String> errorList;
	public boolean errorOnLine;
	public HashMap<String,String> newLineMap;
	
	public void setHeaderToken(HeaderTokenizer headerToken) {
		this.headerToken = headerToken;
		this.errorList = new ArrayList<String>();
		this.newLineMap = new HashMap<String,String>();
	}
	
	public void setNewLine(String line, StringBuilder lineBuilder) {	
		this.line = line;
		//this.lineBuilder = lineBuilder;
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
		/* Converting bad date values to NULL_{COL_NAME} for maintaining consistency */
		this.normalizeDateCols();
		/* Converting bad mobile number values to INVALID_MOBILE for maintaining consistency */
		this.normalizeMobileCol();
		StringBuilder sb = new StringBuilder("");
		for(int j=0;j<this.cols.length;j++) {
			sb.append(this.cols[j]);
			if(j!=(this.cols.length-1)) {
				sb.append(this.headerToken.source.separator);	
			}
		}
		
		this.lineBuilder = sb;
		//this.line = sb.toString();
	}
	
	
	
	public void normalizeDateCols() {
		if(headerToken.source.dateColoumn==null || headerToken.source.dateColoumn.equals(""))
			return;
		
		String[] dateCols = this.headerToken.source.dateColoumn.split(",");
		try {
			
			int[] dateColsIndex = new int[dateCols.length];
			int k =0;
			for(String dateCol: dateCols) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(dateCol.equals(headerToken.originalHeaderCols[i])) {
						dateColsIndex[k] = i; k++;
						break;
					}
				}
			}

			for(int colIndex:dateColsIndex) {
				int num =0;
				String originalValue = this.cols[colIndex];
				String dateValue = this.cols[colIndex].trim();	
				if(dateValue.equals("") || dateValue.equals(" ") || dateValue.equals("NULL")) {
					this.cols[colIndex] = "NULL_"+dateCols[num];
					}
				num++;
				}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in change date format: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in change date format: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in change date format: "+ line);
		}
	}
	
	public void normalizeMobileCol() {
		if (headerToken.source.mobileColumn == null)
			return;
		
		String mobileCol = this.headerToken.source.mobileColumn;
		String mobileColValue = "";
		int mobileColIndex = -1;
		
		try {
			if (mobileCol != null) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(mobileCol.equals(headerToken.originalHeaderCols[i])) {
						mobileColValue = this.cols[i].trim();
						mobileColIndex = i;
						break;
					}
				}
				
				String numericValue = mobileColValue.replaceAll("[^0-9]", "");
				if (StringUtils.isBlank(numericValue) || !StringUtils.isNumericSpace(numericValue)) {
					this.cols[mobileColIndex] = "INVALID_MOBILE";
				} else {
					if (headerToken.source.countryCode != null) {
						this.cols[mobileColIndex] = headerToken.source.countryCode + numericValue;
					} else {
						this.cols[mobileColIndex] = numericValue;
					}
				}
			}		
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in change mobile format: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in change mobile format: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in change mobile format: "+ line);
		}
					
	}
	
	public void dropColumn() {
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
			
			StringBuilder afterDroppedCols = new StringBuilder();
			int mapLen = 0;
			for(Map.Entry<Integer, String> entry:map.entrySet()) {
				if(mapLen==map.size()-1)
					afterDroppedCols.append(entry.getValue());
				else{
					afterDroppedCols.append(entry.getValue()+headerToken.source.separator);
					mapLen++;
				}
				
			}
			this.lineBuilder = afterDroppedCols;
	
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in droppin columns: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in droppin columns: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in droppin columns: "+ line);
		}
		
	}
	
	public void changeDateFormat() {
		if(headerToken.source.dateColoumn==null || headerToken.source.dateColoumn.equals(""))
			return;
		
		try {
			
			String[] dateCols = this.headerToken.source.dateColoumn.split(",");
			int[] dateColsIndex = new int[dateCols.length];
			int k =0;
			for(String dateCol: dateCols) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(dateCol.equals(headerToken.originalHeaderCols[i])) {
						dateColsIndex[k] = i; k++;
						break;
					}
				}
				if(dateColsIndex[k-1] == 0) {
					dateColsIndex[k-1] = -1;
				}
			}
			
			for(int colIndex:dateColsIndex) {
				if(colIndex == -1) {
					continue;
				}

				String originalValue = this.cols[colIndex];
				String dateValue = this.cols[colIndex].trim();			
				if(!dateValue.equals("") && !dateValue.equals(" ")) {
					if(dateValue.equals("NULL") || dateValue.contains("NULL")) { // junk date like "  -  -  " or "NULL_{DATE_COL}"
						/*int startInd = lineBuilder.indexOf(dateValue);
						String replace = "";
						Rule[] rules = this.headerToken.source.getRules();
						for(Rule r:rules) {
							if(r.ruleName.equals("adjustdate") && r.sourceColoumn.equals(this.headerToken.originalHeaderCols[colIndex])) {				
								replace = replace + this.headerToken.source.separator;
							}
							if(r.ruleName.equals("adjustdateelement") && r.sourceColoumn.equals(this.headerToken.originalHeaderCols[colIndex])) {
								replace = replace + this.headerToken.source.separator;
							}
						}
						lineBuilder.replace(startInd, startInd + dateValue.length(),replace );*/
					}
					else {
						SimpleDateFormat formatter1 = null;
						
						/* This is a fix for multiple date formats in the source file 12/9/2018*/
						String currDateFormat = "";
						if (this.headerToken.source.currentDateFormat!= null && this.headerToken.source.currentDateFormat.contains(",")) {
							String[] currentDateFormats = this.headerToken.source.currentDateFormat.split(",");
							boolean error = false;
							for (String format:currentDateFormats) {
								formatter1 = new SimpleDateFormat(format);
								error = false;
								try {
									formatter1.parse(dateValue);
								}catch(ParseException e) {
									error = true;
								}
								if (!error) {
									currDateFormat = format;
								}
							}
						}else {
							currDateFormat = this.headerToken.source.currentDateFormat;
						}
						String convertDateFormat = this.headerToken.source.convertDateFormat;
						if(dateValue.length()>9) {
							formatter1 = new SimpleDateFormat(currDateFormat);
							
						}else {
							if(currDateFormat.equals("MM-dd-yyyy")) { 
								formatter1 = new SimpleDateFormat("MM-dd-yy");
								
							}	
							else if(currDateFormat.equals("MM/dd/yyyy")) {
								String[] mmdd = dateValue.split("/");
								if(mmdd[0].length()==1 && mmdd[1].length()==1)
									formatter1 = new SimpleDateFormat("M/d/yyyy");
								if(mmdd[0].length()==2 && mmdd[1].length()==1)
									formatter1 = new SimpleDateFormat("MM/d/yyyy");
								if(mmdd[0].length()==1 && mmdd[1].length()==2)
									formatter1 = new SimpleDateFormat("M/dd/yyyy");
							}else {
								formatter1 = new SimpleDateFormat(currDateFormat);
							}
								
						}
						 
						Date oldCreateDate;
						try {
							oldCreateDate = (Date) formatter1.parse(dateValue);
						
							SimpleDateFormat newCreateDate = new SimpleDateFormat(convertDateFormat);
							int startInd = lineBuilder.indexOf(originalValue);
							String cDate = newCreateDate.format(oldCreateDate);
					
							lineBuilder.replace(startInd, startInd + originalValue.length(),cDate );
							} catch (ParseException e) {
							System.out.println(dateValue +" parsing error on line "+ line);
							}
						}
					}	
				}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in change date: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in change date: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in change date: "+ line);
		}
		
		
		
	}
	
	public void splitColumn(Rule rule) {
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
	
	public void splitAddress(Rule rule)throws StringIndexOutOfBoundsException {
		
		int maxIterationCount = 1;
		if(rule.secondAdreessString!=null) {
			maxIterationCount = 2;
		}
		int addressColumnIndex = -1;
		String addressValue = null;
		String addressLikeString = null;
		for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
			if(rule.sourceColoumn.equals(headerToken.originalHeaderCols[i])) {
				addressColumnIndex = i; 
				break;
			}
		}
		
		int currentIterationCount = 1;
		boolean success = true;
		while(success) {
			addressValue = cols[addressColumnIndex];
			addressLikeString = rule.adreessString;
			if(currentIterationCount==2) {
				addressLikeString = rule.secondAdreessString;
			}
			String[] destColValues = new String[rule.destinationColumn.length];
			Arrays.fill(destColValues, "");
				
			if(addressValue.equals("") || addressValue.equals(" ")) {
				//TODO:
				success = false;
				System.out.println("No address value in splitaddress: "+ line);
				errorList.add(line);
				this.errorOnLine = true;
			}else {
			
				try {
					int currDigit = -1,startInd = 0;
					StringBuilder sb = new StringBuilder();
					for(int i=0;i<addressLikeString.length();i++) {
						char currentChar = addressLikeString.charAt(i);
						if(Character.isDigit(currentChar)) {
							currDigit = Character.getNumericValue(currentChar);
							
							if(i==addressLikeString.length()-1) {
								
								destColValues[currDigit-1] = addressValue.substring(startInd,addressValue.length());
							}
						}else {
							if(currDigit!=-1) {
								sb.append(currentChar);
								if(!Character.isDigit(addressLikeString.charAt(i+1))){
									
									
								}else {
									
									int currInd = addressValue.indexOf(sb.toString(), startInd==0?0:startInd);
									destColValues[currDigit-1] = addressValue.substring(startInd, currInd);
									startInd = currInd + sb.length();
									sb.setLength(0);
								}		
							}
							
						}
					}
					
					String finalString = "";
					for(String s:destColValues) {
						
						finalString = finalString + s.replace(",", "") + headerToken.source.separator;
					}
					
					int startIndex = lineBuilder.indexOf(addressValue);
					if(!finalString.equals("")) {
						lineBuilder.replace(startIndex, startIndex + addressValue.length() + 1, finalString);
					}else {
						String sepa = headerToken.source.separator;
						for(String col:rule.destinationColumn) {
							finalString = finalString + " " + sepa;
						}
						lineBuilder.replace(startIndex, startIndex + addressValue.length() + 1, finalString);
					}	
					
					return;
					
				}catch(ArrayIndexOutOfBoundsException ae) {
					if(currentIterationCount<maxIterationCount) {
						currentIterationCount++;
					}else {
						success = false;
						System.out.println("[AE]Error in splitaddress: "+ line);
						errorList.add(line);
						this.errorOnLine = true;
					}

				}catch(NullPointerException ne) {
					if(currentIterationCount<maxIterationCount) {
						currentIterationCount++;
					}else {
						success = false;
						System.out.println("[NP]Error in splitaddress: "+ line);
						errorList.add(line);
						this.errorOnLine = true;
					}
				}catch(StringIndexOutOfBoundsException se) {
					if(currentIterationCount<maxIterationCount) {
						currentIterationCount++;
					}else {
						success = false;
						System.out.println("[SE]Error in splitaddress: "+ line);
						errorList.add(line);
						String finalString = "";
						int startIndex = lineBuilder.indexOf(addressValue);
						String sepa = headerToken.source.separator;
						for(String col:rule.destinationColumn) {
							finalString = finalString + " " + sepa;
						}
						lineBuilder.replace(startIndex, startIndex + addressValue.length() + 1, finalString);
					}	
				}
			}
		}
		
		
	}
	
	public void addDate(Rule rule) {

	if(rule.sourceColoumn.equals("NULL")) {
			Date date = new Date();
			if(this.headerToken.source.convertDateFormat!=null) {
				SimpleDateFormat f = new SimpleDateFormat(this.headerToken.source.convertDateFormat);
				lineBuilder.append(headerToken.source.separator +f.format(date));
			}
			else {
				SimpleDateFormat f = new SimpleDateFormat("yyyy-MM-dd");
				lineBuilder.append(headerToken.source.separator +f.format(date));
			}	
		}	
	}
	
	public void calculateDays(Rule rule, boolean isCalculateYears) {
		try {
			String dateCol1 = rule.calDaysColumn1;
			if(isCalculateYears) {
				dateCol1 = rule.calYearsColumn;
			}
			String dateCol2 = rule.calDaysColumn2;
			int dateCol1Index;
			int dateCol2Index;
			String date1Value = "";
			String date2Value = null;
			
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(dateCol1.equals(headerToken.originalHeaderCols[i])) {
					dateCol1Index = i;
					date1Value = this.cols[dateCol1Index];
					break;
				}
			}
			if(dateCol2!=null) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(dateCol2.equals(headerToken.originalHeaderCols[i])) {
						dateCol2Index = i;
						date2Value = this.cols[dateCol2Index];
						break;
					}
				}
			}
			
			if(!date1Value.equals("") && !date1Value.equals(" ") && !date1Value.equals("NULL") && !date1Value.contains("NULL")) {
				SimpleDateFormat formatter1 = null;
				String currDateFormat = "";
				if (this.headerToken.source.currentDateFormat!= null && this.headerToken.source.currentDateFormat.contains(",")) {
					String[] currentDateFormats = this.headerToken.source.currentDateFormat.split(",");
					boolean error = false;
					for (String format:currentDateFormats) {
						formatter1 = new SimpleDateFormat(format);
						error = false;
						try {
							formatter1.parse(date1Value);
						}catch(ParseException e) {
							error = true;
						}catch (DateTimeParseException de) {
							error = true;
						}
						if (!error) {
							currDateFormat = format;
						}
					}
				}else {
					currDateFormat = this.headerToken.source.currentDateFormat;
				}
				
				String convertDateFormat = this.headerToken.source.convertDateFormat;
				if(convertDateFormat!=null && convertDateFormat.equals("yyyy-MM-dd")) {
					
					if(currDateFormat.equals("yyyyMMdd")) {
						SimpleDateFormat ft = new SimpleDateFormat(currDateFormat);
						Date dt = ft.parse(date1Value);
						LocalDate date1 = LocalDate.parse(date1Value, DateTimeFormatter.BASIC_ISO_DATE);
						LocalDate date2 = null;
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							date2 = LocalDate.parse(date2Value, DateTimeFormatter.BASIC_ISO_DATE);
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
						return;
					} else if(currDateFormat.equals("yyyy/MM/dd")) {
						SimpleDateFormat ft = new SimpleDateFormat(currDateFormat);
						Date dt = ft.parse(date1Value);
						LocalDate date1 = LocalDate.parse(date1Value, DateTimeFormatter.ofPattern("yyyy/MM/dd"));
						LocalDate date2 = null;
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							date2 = LocalDate.parse(date2Value, DateTimeFormatter.BASIC_ISO_DATE);
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
						return;
					} else if (date1Value.contains("/") && !currDateFormat.contains(" ")) {  // BUG FIX 1/14/2019
						String[] groups = date1Value.split("/");
						StringBuffer sb = new StringBuffer("");
						sb.append(groups[0].length() == 1?"M":"MM");
						sb.append(groups[1].length() == 1?"/d":"/dd");
						sb.append(groups[2].length() == 2?"/yy":"/yyyy");
						System.out.println(sb.toString());
						SimpleDateFormat ft = new SimpleDateFormat(sb.toString());
						Date dt = ft.parse(date1Value);
						LocalDate date1 = LocalDate.parse(date1Value, DateTimeFormatter.ofPattern(sb.toString()));
						LocalDate date2 = null;
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							date2 = LocalDate.parse(date2Value, DateTimeFormatter.BASIC_ISO_DATE);
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
						return;
					}else if (currDateFormat.contains(" ")) {  // BUG FIX 2/07/2019
						/*String[] groups = date1Value.split("/");
						StringBuffer sb = new StringBuffer("");
						sb.append(groups[0].length() == 1?"M":"MM");
						sb.append(groups[1].length() == 1?"/d":"/dd");
						sb.append(groups[2].length() == 2?"/yy":"/yyyy");
						System.out.println(sb.toString());*/
						SimpleDateFormat ft = new SimpleDateFormat(currDateFormat);
						Date dt = ft.parse(date1Value);
						LocalDate date1 = LocalDate.parse(date1Value, DateTimeFormatter.ofPattern(currDateFormat));
						LocalDate date2 = null;
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							date2 = LocalDate.parse(date2Value, DateTimeFormatter.BASIC_ISO_DATE);
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
						return;
					}
					
					
					
					
					
					
					
					
					String[] mmddyy = date1Value.trim().split("-");
					if(!mmddyy[0].contains(" ") && !mmddyy[1].contains(" ") && !mmddyy[2].contains(" ")) {
						LocalDate date2 = LocalDate.now();
						LocalDate date1 = null;
						if(currDateFormat.equals("yyyy-MM-dd")) {
							date1 = LocalDate.of(Integer.parseInt(mmddyy[0]), Integer.parseInt(mmddyy[1]), Integer.parseInt(mmddyy[2]));
						}else if(currDateFormat.equals("MM-dd-yyyy")) {
							if(mmddyy[2].length()==2) {
								mmddyy[2] = "19" + mmddyy[2];
							}
							date1 = LocalDate.of(Integer.parseInt(mmddyy[2]), Integer.parseInt(mmddyy[0]), Integer.parseInt(mmddyy[1]));
						}
						
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							String[] mmddyy2 = date2Value.split("-");
							if(!mmddyy2[0].contains(" ") && !mmddyy2[1].contains(" ") && !mmddyy2[2].contains(" ")) {
								date2 = LocalDate.of(Integer.parseInt(mmddyy2[2]), Integer.parseInt(mmddyy2[0]), Integer.parseInt(mmddyy2[1]));
							}
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
					}else {
						lineBuilder.append(headerToken.source.separator);
					}
				}else if(convertDateFormat!=null && convertDateFormat.equals("yyyy/MM/dd")) {
					
					if(currDateFormat.equals("yyyyMMdd")) {
						SimpleDateFormat ft = new SimpleDateFormat(currDateFormat);
						Date dt = ft.parse(date1Value);
						LocalDate date1 = LocalDate.parse(date1Value, DateTimeFormatter.BASIC_ISO_DATE);
						LocalDate date2 = null;
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							date2 = LocalDate.parse(date2Value, DateTimeFormatter.BASIC_ISO_DATE);
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
						return;
					}
					
					
					String[] mmddyy = date1Value.trim().split("/");
					if(!mmddyy[0].contains(" ") && !mmddyy[1].contains(" ") && !mmddyy[2].contains(" ")) {
						LocalDate date2 = LocalDate.now();
						LocalDate date1 = null;
						if(currDateFormat.equals("yyyy/MM/dd")) {
							date1 = LocalDate.of(Integer.parseInt(mmddyy[0]), Integer.parseInt(mmddyy[1]), Integer.parseInt(mmddyy[2]));
						}else if (currDateFormat.equals("MM/dd/yyyy")) {
							date1 = LocalDate.of(Integer.parseInt(mmddyy[2]), Integer.parseInt(mmddyy[0]), Integer.parseInt(mmddyy[1]));
						}
						
						if(date2Value!=null && !date2Value.equals("") && !date2Value.equals(" ") && !date2Value.contains("NULL")) {
							String[] mmddyy2 = date2Value.split("/");
							if(!mmddyy2[0].contains(" ") && !mmddyy2[1].contains(" ") && !mmddyy2[2].contains(" ")) {
								date2 = LocalDate.of(Integer.parseInt(mmddyy2[2]), Integer.parseInt(mmddyy2[0]), Integer.parseInt(mmddyy2[1]));
							}
						}
						else {
							date2 = LocalDate.now();
						}
						
						Period p = Period.between(date1, date2);
						long days = ChronoUnit.DAYS.between(date1, date2);
						if(isCalculateYears) {
							days = ChronoUnit.YEARS.between( date1 , date2 );
						}
						lineBuilder.append(headerToken.source.separator+ days);
					}else {
						lineBuilder.append(headerToken.source.separator);
					}
				}
	
			}else {
				lineBuilder.append(headerToken.source.separator);
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in calculatedays: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in calculatedays: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in calculatedays: "+ line);
		} catch (ParseException e) {
			System.out.println("[PE]Error in calculatedays: "+ line);
		} catch(DateTimeParseException e) {
			System.out.println("[DE]Error in calculatedays: "+ line);
		}
		
	}
	
	public void stripPartial() {
		if(headerToken.source.stripPartial==null || headerToken.source.stripPartial.equals(""))
			return;
		
		try {
			String[] strip = this.headerToken.source.stripPartial.split(":");
			String stripColumnName = strip[0];
			String stripSeparator = strip[1];
			String badValue = strip[2];
			int stripColunmIndex;
			String stripColumnValue = "";
			
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(stripColumnName.equals(headerToken.originalHeaderCols[i])) {
					stripColumnValue = this.cols[i];
					stripColunmIndex = i;
					break;
				}
			}
			
			if(!stripColumnValue.contains(stripSeparator)) {
				if(stripColumnValue.contains(badValue)) {
					int startInd = this.lineBuilder.indexOf(stripColumnValue);
					this.lineBuilder.replace(startInd, startInd +stripColumnValue.length(), "");
				}
			}else {
				String[] str = stripColumnValue.split(stripSeparator);
				String validValue = "";
				for(String s:str) {
					if(!s.contains(badValue))
						validValue = s;
					
				}
				if(!validValue.equals("")) {
					int startInd = lineBuilder.indexOf(stripColumnValue);
					this.lineBuilder.replace(startInd, startInd +stripColumnValue.length(), validValue);
				}
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in strippartial: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in strippartial: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in strippartial: "+ line);
		}
	}
	
	/* For the golbal rule @mixedcase*/
	public void mixedCase() {
		if(this.headerToken.source.getMixedCase()==null || this.headerToken.source.getMixedCase().equals(" ") || this.headerToken.source.getMixedCase().equals(""))
			return;
		
		String[] colNames = this.headerToken.source.getMixedCase().split(",");
		int[] colIndex = new int[colNames.length];
		String[] colValues = new String[colNames.length];
		
		
		try {
			int k=0;
			for(String col:colNames) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(col.equals(this.headerToken.originalHeaderCols[i])) {
						colValues[k] = this.cols[i];
						k++;
						break;
					}
				}
			}
			
			for(String colValue:colValues) {
				
				if((!colValue.equals("") || !colValue.equals("")) && colValue.length()>=2) {
					String capitalized = colValue.substring(0,1).toUpperCase() + colValue.substring(1).toLowerCase();
					int startInd = lineBuilder.indexOf(colValue);
					lineBuilder.replace(startInd, startInd + colValue.length(), capitalized);
				}
				
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in mixedcase: "+ line);
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in mixedcase: "+ line);
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in mixedcase: "+ line);
		}	
	}
	
	public void selectSingle(Rule rule) {
		if(rule==null || rule.columnList==null)
			return;
		
		String finalString = "";
		switch(rule.selectSingleCondition) {
		
		case "notnull":
			String colVal = "";
			for(String colName:rule.columnList) {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(colName.equals(headerToken.originalHeaderCols[i])) {
						colVal = this.cols[i];
						break;
					}
				}
				
				if(!colVal.equals("") && !colVal.equals(" ")) {
					finalString = colVal;
					break;
				}
			}
			break;
			
		default:
			break;
		//TODO: For future conditions to be implemented.
			
		}
		
		if(rule.sourceColoumn.equals("NULL")) {
			lineBuilder.append(this.headerToken.source.separator + finalString);
		}
		
	}
	
	public void addConstant(Rule rule) {
		if(rule==null || rule.constant==null)
			return;
		
		lineBuilder.append(this.headerToken.source.separator + rule.constant);
	}
	
	// Currently supports only single column
	public boolean dropRecord() {
		
		
		try {	
		// If email is null, return true;
		if(headerToken.source.emailColumn!=null) {
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(headerToken.source.emailColumn.equals(headerToken.originalHeaderCols[i])) {
					
					if(this.cols[i].equals("") || this.cols[i].equals(" "))
						return true;
					break;
				}
			}
		}
		
		if(headerToken.source.dropRecord==null || headerToken.source.dropRecord.equals(""))
			return false;
		
		
			String[] dropRecords = this.headerToken.source.dropRecord.split(","); 
			String badValue = dropRecords[0];
			if(badValue.equals("blank")) {
				badValue = "";
			}
			String dropRecordCol = dropRecords[1];
			int dropRecordColIndex;
			if(dropRecords[0].contains("\"")) {
				badValue = dropRecords[0].replaceAll("\"", "");
			}
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(dropRecordCol.equals(headerToken.originalHeaderCols[i])) {
					dropRecordColIndex = i;
					if(this.cols[i].contains(badValue) || this.cols[i].contains(badValue.toLowerCase()) || this.cols[i].contains(badValue.toUpperCase()))
						return true;
					break;
				}
			}
			
			
			
			
			
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println("[AE]Error in droprecord: "+ line);
			return false;
		}catch(NullPointerException ne) {
			System.out.println("[NP]Error in droprecord: "+ line);
			return false;
		}catch(StringIndexOutOfBoundsException se) {
			System.out.println("[SE]Error in droprecord: "+ line);
			return false;
		}
		
		return false;
	}

	public void adjustDate(Rule rule) {
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
		SimpleDateFormat formatterOld = null,formatterNew=null;
		Date old = null;
		String oldDateConverted=null;
		formatterOld = new SimpleDateFormat(this.headerToken.source.currentDateFormat);
		if(dateValueOld.contains(" ") || dateValueOld.equals("NULL") || dateValueOld.contains("NULL")) { // junk date like "  -  -  "
			int startInd = lineBuilder.indexOf(dateValueOld);
			lineBuilder.replace(startInd, startInd + dateValueOld.length(),this.headerToken.source.separator );
			// Taken care of junk dates in the changeDateFormat method.
			return;
		}else {
			try {
				oldDateConverted = this.convertDateFormat(dateValueOld);
				formatterNew = new SimpleDateFormat(this.headerToken.source.convertDateFormat);
				//old = formatterOld.parse(dateValueOld);
				Calendar cal = Calendar.getInstance();
				cal.setTime(formatterNew.parse(oldDateConverted));
				if(minus) {
					cal.add(Calendar.DATE, -days);
				}
				else {
					cal.add(Calendar.DATE, days);
				}
				//formatterNew = new SimpleDateFormat(this.headerToken.source.convertDateFormat);
				Date newDate = cal.getTime();
				dateValueNew = formatterNew.format(newDate);
				
			}catch (ParseException e) {
				System.out.println(dateValueOldCol +" parsing error on line "+ line);
			}
		}
		
		int startInd = lineBuilder.indexOf(oldDateConverted);
		lineBuilder.replace(startInd, startInd + oldDateConverted.length(),oldDateConverted+this.headerToken.source.separator+dateValueNew );
		
	}
	
	public void adjustDateElement(Rule rule) {
		if(rule==null || rule.adjustDateElement==null)
			return;
		
		String[] arr = rule.adjustDateElement.split(",");
		String element = arr[0];
		String newVal = arr[1];
		String dateValueOld = null,dateValueNew="";
		String dateValueOldCol = rule.sourceColoumn;
		String[] dest = rule.destinationColumn;
		String dateValueNewCol = dest[0];
		try {
			for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
				if(dateValueOldCol.equals(headerToken.originalHeaderCols[i])) {
					int dateColIndex = i;
					dateValueOld = this.cols[dateColIndex];
					break;
				}
			}
		}catch(ArrayIndexOutOfBoundsException ae) {
			System.out.println(dateValueOldCol +" parsing error on line "+ line);
			//ae.printStackTrace();
			return;
		}
		
		
		SimpleDateFormat formatterOld = null,formatterNew=null;
		Date old = null;
		String oldDateConverted=null;
		formatterOld = new SimpleDateFormat(this.headerToken.source.currentDateFormat);
		
		if(dateValueOld.equals("") || dateValueOld.contains(" ") || dateValueOld.equals("NULL") || dateValueOld.contains("NULL")) { // junk date like "  -  -  "
			int startInd = lineBuilder.indexOf(dateValueOld);
			lineBuilder.replace(startInd, startInd + dateValueOld.length(),this.headerToken.source.separator );
			// Taken care of junk dates in the changeDateFormat method.
			return;
		}else {
			try {
				
				Calendar cal = Calendar.getInstance();
				oldDateConverted = this.convertDateFormat(dateValueOld);
				formatterNew = new SimpleDateFormat(this.headerToken.source.convertDateFormat);
				if(element.equals("year")) {
					int currentYear = Calendar.getInstance().get(Calendar.YEAR);
					cal.setTime(formatterNew.parse(oldDateConverted));
					
					if(newVal.equals("current")) {
						cal.set(Calendar.YEAR, currentYear);
					}else { // any constant value like 2018, 1999
						cal.set(Calendar.YEAR, Integer.parseInt(newVal));
					}
				}
				
				if(element.equals("day")) {
					int currentDay = Calendar.getInstance().get(Calendar.DAY_OF_MONTH);
					if(newVal.equals("current")) {
						cal.set(Calendar.DAY_OF_MONTH,currentDay);
					}else { // any constant value like 31, 01
						cal.set(Calendar.DAY_OF_MONTH, Integer.parseInt(newVal));
					}
				}
				
				if(element.equals("month")) {
					
					if(newVal.equals("current")) {
						
					}else { // any constant value like 01, 12
						
					}
				}
				
				dateValueNew = formatterNew.format(cal.getTime());
				
			}catch (ParseException e) {
				System.out.println(dateValueOldCol +" parsing error on line "+ line);
			}
		}	
		int startInd = lineBuilder.indexOf(oldDateConverted);
		lineBuilder.replace(startInd, startInd + oldDateConverted.length(),oldDateConverted+this.headerToken.source.separator+dateValueNew );
		
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
	
	public void formatMobileNumbers() {
		if ( headerToken.source.mobileColumn == null) {
			return;
		} else {
			String mobileColName = headerToken.source.getMobileColumn();
			String currentMobileNumber = "";
			try {
				for(int i=0;i<this.headerToken.originalHeaderCols.length;i++) {
					if(mobileColName.equals(headerToken.originalHeaderCols[i])) {
						int mobileColIndex = i;
						currentMobileNumber = this.cols[mobileColIndex].trim();
						break;
					}
				}
			}catch(ArrayIndexOutOfBoundsException ae) {
				System.out.println(currentMobileNumber +" parsing error on line "+ line);
				//ae.printStackTrace();
				return;
			}
			
			if (currentMobileNumber.equals("INVALID_MOBILE")) {
				int startInd = lineBuilder.indexOf("INVALID_MOBILE");
				lineBuilder.replace(startInd, startInd + "INVALID_MOBILE".length(),"" );
			} else if (!StringUtils.isEmpty(currentMobileNumber) && StringUtils.isAlphanumeric(currentMobileNumber)) {
				StringBuilder number = new StringBuilder(currentMobileNumber.replaceAll("[^0-9]", ""));
				
				if (headerToken.source.countryCode != null) {
					number.append(headerToken.source.countryCode);
				}
				int startInd = lineBuilder.indexOf(currentMobileNumber);
				lineBuilder.replace(startInd, startInd + currentMobileNumber.length(), number.toString() );
			}
			
			
			
		}
			
					
	}
}

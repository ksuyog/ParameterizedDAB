package aws.dynamodb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AmazonDynamoDBException;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;

public class PopulateDB {
	public AWSCredentials creds;
	public AmazonDynamoDB client;
	public DynamoDB dynamoDB;
	public String[] columns;
	public int[] columnIndex;
	
	public PopulateDB(String rawDatabseColumns) {
		try {
			//this.creds = new ProfileCredentialsProvider("suyog").getCredentials();
			this.client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.US_EAST_2).build();
	        this.dynamoDB = new DynamoDB(client);
	        this.columns = rawDatabseColumns.split(",");
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. ",e);
        }	
	}
	
public void createTable(String rawDatabaseColumns) {
		String[] columns = rawDatabaseColumns.split(",");
        List<AttributeDefinition> attributeDefinitions= new ArrayList<AttributeDefinition>();
        attributeDefinitions.add(new AttributeDefinition().withAttributeName(columns[0]).withAttributeType("S"));
        
        
        List<KeySchemaElement> keySchema = new ArrayList<KeySchemaElement>();
        keySchema.add(new KeySchemaElement().withAttributeName(columns[0]).withKeyType(KeyType.HASH));
                
        CreateTableRequest request = new CreateTableRequest()
                .withTableName("Temp")
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(new ProvisionedThroughput()
                    .withReadCapacityUnits(100L)
                    .withWriteCapacityUnits(100L));

        Table table = dynamoDB.createTable(request);
		
        try {
			table.waitForActive();
			System.out.println("Created: "+table.getTableName());
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

public void populateFromCSV(File csvFile,String rawDatabaseColumns) {
	BufferedReader reader = null;
	List<String> errorList = new ArrayList<String>();
	String line = "";
	String headerLine = "";
	String separater = ",";
	int totalLines = 0;
	Table table = this.dynamoDB.getTable("Temp");
	try {
		reader = new BufferedReader(new FileReader(csvFile));
		headerLine = reader.readLine();
		String[] headers = headerLine.split(",");
		String[] columns = rawDatabaseColumns.split(",");
		this.columns = columns;
		int[] columnIndex = new int[columns.length];
		
		int k = 0,i = 0,m = 0;
		for(k=0;k<columns.length;k++) {
			for(i=0;i<headers.length;i++) {
				if(columns[k].equals(headers[i])) {
					columnIndex[m] = i;m++;break;
				}
			}
		}
		this.columnIndex = columnIndex;
		
		TableWriteItems tableWriteItems = new TableWriteItems("Temp");
		List<Item> putItemList = new ArrayList<Item>();
		String primaryKey;
		while((line = reader.readLine())!=null) {
			String[] cols = line.split(separater);
			if(cols.length<columnIndex[0]+1) 
				continue;
			
			primaryKey = cols[columnIndex[0]];	
		
			try {
				
			if(!primaryKey.equals("") || !primaryKey.equals(" ")) {
				
					if(putItemList.isEmpty() || putItemList.size()<24) {
						Item item = new Item();
						if(primaryKey.length()==9) {
							int len = primaryKey.length();
							item.withPrimaryKey(columns[0],primaryKey.substring(1,len-1 ));
						}
						else {
							item.withPrimaryKey(columns[0],primaryKey);
						}
						
						for(int j=1;j<columns.length;j++) {
							item.withString(columns[j],cols[columnIndex[j]] );
						}

						putItemList.add(item);			
						}else {
							Item item = new Item();
							item.withPrimaryKey(columns[0],primaryKey);
							for(int j=1;j<columns.length;j++) {
								item.withString(columns[j],cols[columnIndex[j]] );
							}
						putItemList.add(item);	
						tableWriteItems.withItemsToPut(putItemList);
						putItemList.clear();
						BatchWriteItemOutcome outcome = this.dynamoDB.batchWriteItem(tableWriteItems);
						
						}
					
				}
			}catch(AmazonDynamoDBException ex) {
					errorList.add(primaryKey);
			}catch(AmazonServiceException se) {
					System.out.println(se.getErrorMessage());
			}
			
		totalLines = totalLines + 1;
	
		}
		
		
		try{
			if(!putItemList.isEmpty()) {
		
			tableWriteItems.withItemsToPut(putItemList);
			
			BatchWriteItemOutcome outcome = this.dynamoDB.batchWriteItem(tableWriteItems);
				}
			}catch(AmazonServiceException se) {
			System.out.println(se.getErrorMessage());
		}

		
		System.out.println("Read "+ totalLines +" lines");
		System.out.println("Error list size: "+ errorList.size());
		reader.close();
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
}

public void deleteTable() {
	
	Table table = this.dynamoDB.getTable("Temp");

	table.delete();

	try {
		table.waitForDelete();
		System.out.println("Deleted..");
	} catch (InterruptedException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
}

public String[] getFromDB(String primaryKey,String[] cols){
	String[] list = null; 	
	Table table = this.dynamoDB.getTable("Temp");

	try {
			Item record = table.getItem(this.columns[0],primaryKey);

			if(record!=null) {
				list = new String[cols.length];
				for(int i=0;i<cols.length;i++) {
					list[i] = record.getString(cols[i]);
				}
			}
		}catch(AmazonServiceException e) {
			System.out.println("Error for barcode: "+primaryKey);
			return list;
		}
	return list;
	}
}

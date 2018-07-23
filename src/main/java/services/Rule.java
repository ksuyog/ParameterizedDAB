package services;

public class Rule {
	public String sourceColoumn;
	public String[] destinationColumn;
	public String ruleName;
	public String splitSeparator;
	public int[] spliElementsToKeep;
	public String separator;
	public String adreessString;
	public String secondAdreessString;
	public int[] coloumnToKeep;
	public String addDate;
	public String calDaysColumn1;
	public String calDaysColumn2;
	public boolean mixedCase;
	public boolean stripMiddleInitial;
	public String transaction_format;
	public String transaction_format_date;
	public String findFromDB;
	public String[] columnList;
	public String selectSingleCondition;
	public String constant;
	public String adjustDate;
	public String adjustDateElement;
	public String calculateYears;
	public String calYearsColumn;
	
	public Rule(String ruleString) {
		String[] array = ruleString.split("\\|");
		String[] ruleParts = null;
		if(array!=null) {
			if(array.length>=1)
				this.sourceColoumn = array[0];
			if(array.length>=2)
				this.destinationColumn = array[1].split(";");
			if(array.length>=3) {
				ruleParts = array[2].split(":");
				this.ruleName = ruleParts[0];
			}
				
			switch(this.ruleName) {
			
				case "splitcolumn":
					this.splitSeparator = ruleParts[1];
					String[] elementsToKeep = ruleParts[2].split(",");
					int i=0;
					this.spliElementsToKeep = new int[elementsToKeep.length];
					for(String s:elementsToKeep) {
						this.spliElementsToKeep[i] = Integer.parseInt(s);i++;
					}
					break;
				case "selectsingle":	
					this.columnList = ruleParts[1].split(",");
					this.selectSingleCondition = ruleParts[2];
					break;
				case "constant":
					this.constant = ruleParts[1];
					break;
				case "splitaddress":
					this.adreessString = ruleParts[1];
					if(ruleParts.length==3)
						this.secondAdreessString = ruleParts[2];
					break;
				case "add_date":
					this.addDate = ruleParts[1];
					break;
				case "calculatedays":
					if(ruleParts.length>=2)
						this.calDaysColumn1 = ruleParts[1];
					if(ruleParts.length>=3)
						this.calDaysColumn2 = ruleParts[2];
					break;
				case "calculateyears":
					if(ruleParts.length>=2)
						this.calYearsColumn = ruleParts[1];
					break;
				case "total&date":
					if(ruleParts.length>=2)
						this.transaction_format = ruleParts[1];
					if(ruleParts.length>=3)
						this.transaction_format_date = ruleParts[2];
					break;
				case "findfromdb":
					if(ruleParts.length>=2)
						this.findFromDB = ruleParts[1];
					break;
					
				case "adjustdate":
					this.adjustDate = ruleParts[1];
					break;
					
				case "adjustdateelement":
					this.adjustDateElement = ruleParts[1];
					break;
					
				default:
					System.out.println("The rule "+ this.ruleName +" is not yet implemented.");
					break;
			}
			if(array.length>=4) {
				if(array[3].equals("mixedcase"))
					this.mixedCase = true;
				if(array[3].equals("stripmiddleinitial"))
					this.stripMiddleInitial = true;
			}
			if(array.length>=5) {
				if(array[4].equals("mixedcase"))
					this.mixedCase = true;
				if(array[4].equals("stripmiddleinitial"))
					this.stripMiddleInitial = true;
			}
			
		}
			
		
	}
}

package services;

public class HeaderTokenizer {

	public String originalHeader;
	public String[] originalHeaderCols;
	public Source source;
	public String finalHeader;
	public String[] finalHeaderCols;

	public HeaderTokenizer(String headerLine, Source source) {
		this.originalHeader = headerLine.replaceAll("\"", "");
		this.source = source;
		if (source.separator.equals(",")) {
			this.originalHeaderCols = originalHeader.split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
			for (int i = 0; i < originalHeaderCols.length; i++) {
				String st = originalHeaderCols[i].replaceAll("\"", "");
				originalHeaderCols[i] = st;
			}
		}

		if (source.separator.equals("|")) {
			this.originalHeaderCols = originalHeader.split("\\|");
			for (int i = 0; i < originalHeaderCols.length; i++) {
				String st = originalHeaderCols[i].replaceAll("\"", "");
				originalHeaderCols[i] = st;
			}
		}

	}

	public StringBuilder readHeader() {
		StringBuilder headerBuilder = new StringBuilder(originalHeader);

		if (this.source.dropColoumn != null && !this.source.dropColoumn.equals("")
				&& !this.source.dropColoumn.equals(" ")) {
			// Drop the @dropcolumns
			String[] dropColNames = source.dropColoumn.split(",");
			for (String col : dropColNames) {
				int startInd = headerBuilder.indexOf(col);
				if (col.equals(originalHeaderCols[originalHeaderCols.length - 1])) // If dropping last col, then replace
																					// separator at the start
					headerBuilder.replace(startInd - 1, startInd + col.length() + 1, "");
				else
					headerBuilder.replace(startInd, startInd + col.length() + 1, "");
			}
		}

		// Replace existing columns with new from each rule
		// Append cols if source col in NULL in a rule
		for (Rule rule : source.rules) {
			int startInd;
			if (!rule.sourceColoumn.equals("NULL")) {
				startInd = headerBuilder.indexOf(rule.sourceColoumn);
				String replace = "";
				for (String dest : rule.destinationColumn) {
					if (rule.ruleName.equals("adjustdate") || rule.ruleName.equals("adjustdateelement")) {
						replace = replace + rule.sourceColoumn + source.separator + dest + source.separator;
					} else {
						replace = replace + dest + source.separator;
					}
				}
				headerBuilder.replace(startInd, startInd + rule.sourceColoumn.length() + 1, replace);
			} else {
				String replace = "";
				for (String dest : rule.destinationColumn) {
					replace = replace + source.separator + dest;
				}
				headerBuilder.append(replace);
			}
		}
		this.finalHeader = headerBuilder.toString();
		if (source.separator.equals(",")) {
			this.finalHeaderCols = headerBuilder.toString().split(",(?=(?:[^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)", -1);
		}

		if (source.separator.equals("|")) {
			this.finalHeaderCols = headerBuilder.toString().split("\\|");
		}

		return headerBuilder;
	}
}

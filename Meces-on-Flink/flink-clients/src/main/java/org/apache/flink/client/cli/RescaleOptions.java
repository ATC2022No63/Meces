package org.apache.flink.client.cli;

import org.apache.commons.lang3.StringUtils;

import org.apache.flink.util.Preconditions;

import org.apache.commons.cli.CommandLine;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.client.cli.CliFrontendParser.RESCALE_MODE_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.RESCALE_PARALLELISM_LIST_OPTION;
import static org.apache.flink.client.cli.CliFrontendParser.RESCALE_PARALLELISM_OPTION;

public class RescaleOptions extends CommandLineOptions {

	private final String[] args;

	private int mode;
	private int globalParallelism;
	private Map<String, Integer> parallelismList = null;

	RescaleOptions(CommandLine line) {
		super(line);
		args = line.getArgs();
		if (line.hasOption(RESCALE_MODE_OPTION.getOpt())) {
			mode = Integer.parseInt(line.getOptionValue(RESCALE_MODE_OPTION.getOpt()));
		} else {
			mode = 0;
		}

		if (line.hasOption(RESCALE_PARALLELISM_OPTION.getOpt())) {
			globalParallelism = Integer.parseInt(line.getOptionValue(RESCALE_PARALLELISM_OPTION.getOpt()));
			parallelismList = new HashMap<>();
		} else {
			globalParallelism = -1;
			if (line.hasOption(RESCALE_PARALLELISM_LIST_OPTION.getOpt())) {
				parseParallelismListFromString(line.getOptionValue(RESCALE_PARALLELISM_LIST_OPTION.getOpt()));
			}
		}
	}

	private void parseParallelismListFromString(String str) {
		try {
			parallelismList = new HashMap<>();
			Preconditions.checkArgument(str.length() >= 2);
			Preconditions.checkArgument(str.charAt(0) == '[');
			Preconditions.checkArgument(str.charAt(str.length() - 1) == ']');
			for (String entryStr : str.substring(1, str.length() - 1).split(",")) {
				entryStr = entryStr.trim();
				int index = entryStr.lastIndexOf(":");
				parallelismList.put(
					StringUtils.strip(entryStr.substring(0, index), "\""),
					Integer.parseInt(entryStr.substring(index + 1)));
			}
		} catch (Exception e) {
			parallelismList = null;
			throw e;
		}
	}

	public String[] getArgs() {
		return args == null ? new String[0] : args;
	}

	public int getMode() {
		return mode;
	}

	public int getGlobalParallelism() {
		return globalParallelism;
	}

	public Map<String, Integer> getParallelismList() {
		return parallelismList;
	}
}

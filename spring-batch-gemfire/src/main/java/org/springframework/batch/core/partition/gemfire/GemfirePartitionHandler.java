package org.springframework.batch.core.partition.gemfire;

import java.io.Serializable;
import java.util.Collection;
import java.util.Set;

import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.StepExecutionSplitter;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.Execution;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.cache.execute.ResultCollector;

public class GemfirePartitionHandler implements PartitionHandler {

	private int gridSize = 1;

	private Region<String, StepExecution> region;

	private String configLocation;

	private String stepName;
	
	public void setRegion(Region<String, StepExecution> region) {
		this.region = region;
	}

	public void setConfigLocation(String configLocation) {
		this.configLocation = configLocation;
	}
	
	public void setStepName(String stepName) {
		this.stepName = stepName;
	}
	
	public void setGridSize(int gridSize) {
		this.gridSize = gridSize;
	}

	public Collection<StepExecution> handle(StepExecutionSplitter stepSplitter, StepExecution masterStepExecution)
			throws Exception {

		Set<StepExecution> split = stepSplitter.split(masterStepExecution, gridSize);
		for (StepExecution stepExecution : split) {
			region.put(stepExecution.getStepName(), stepExecution);
		}
		
		Execution execution = FunctionService.onRegion(region);
		ResultCollector<? extends Serializable> collector = execution.execute(new GemfirePartitionFunction(configLocation, stepName));

		@SuppressWarnings("unchecked")
		Collection<StepExecution> result = (Collection<StepExecution>) collector.getResult();

		for (StepExecution stepExecution : split) {
			region.remove(stepExecution.getStepName());
		}

		return result;

	}

}

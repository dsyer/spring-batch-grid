package org.springframework.batch.core.partition.gemfire;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobInterruptedException;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.UnexpectedJobExecutionException;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;
import com.gemstone.gemfire.cache.execute.ResultSender;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class GemfirePartitionFunction extends FunctionAdapter {

	private static Log logger = LogFactory.getLog(GemfirePartitionFunction.class);

	private String configLocation;

	private String stepName;

	private Step step;

	public GemfirePartitionFunction(String configLocation, String stepName) {
		this.configLocation = configLocation;
		this.stepName = stepName;
	}

	@Override
	public void execute(FunctionContext context) {
		execute(context.<StepExecution> getResultSender(), extract((RegionFunctionContext) context));
	}

	private void execute(ResultSender<StepExecution> sender, List<StepExecution> executors) {
		int count = 1;
		for (StepExecution executor : executors) {
			count++;
			if (count < executors.size()) {
				sender.sendResult(execute(executor));
			} else {
				sender.lastResult(execute(executor));
			}
		}
	}

	private StepExecution execute(StepExecution stepExecution) {
		try {
			getStep().execute(stepExecution);
		}
		catch (JobInterruptedException e) {
			stepExecution.getJobExecution().setStatus(BatchStatus.STOPPING);
			throw new UnexpectedJobExecutionException("TODO: this should result in a stop", e);
		}
		return stepExecution;
	}

	private List<StepExecution> extract(RegionFunctionContext context) {
		Map<String, StepExecution> data = PartitionRegionHelper.getLocalDataForContext(context);
		List<StepExecution> list = new ArrayList<StepExecution>();
		for (Entry<String, StepExecution> entry : data.entrySet()) {
			logger.debug("Extract:" + entry);
			list.add(entry.getValue());
		}
		return list;
	}

	@Override
	public String getId() {
		return getClass().getSimpleName();
	}

	private Step getStep() {
		if (step == null) {
			synchronized (configLocation) {
				if (step == null) {
					step = (Step) new ClassPathXmlApplicationContext(configLocation).getBean(stepName, Step.class);
				}
			}
		}
		return step;
	}

}

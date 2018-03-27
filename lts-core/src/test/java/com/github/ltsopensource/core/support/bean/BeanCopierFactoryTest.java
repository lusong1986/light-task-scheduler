package com.github.ltsopensource.core.support.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.github.ltsopensource.core.commons.utils.BeanUtils;
import com.github.ltsopensource.core.compiler.AbstractCompiler;
import com.github.ltsopensource.core.domain.Job;
import com.github.ltsopensource.core.domain.JobType;
import com.github.ltsopensource.core.support.JobUtils;
import com.github.ltsopensource.queue.domain.JobDependencyPo;
import com.github.ltsopensource.queue.domain.JobPo;

/**
 * @author Robert HG (254963746@qq.com) on 4/2/16.
 */
public class BeanCopierFactoryTest {

	JobPo jobPo;

	@Before
	public void init() {
		jobPo = new JobPo();
		jobPo.setJobId("dsfdasf");
		jobPo.setTaskId("dfasfs");
		jobPo.setPriority(5);
		
		//jobPo 里面的几个list、map对象不能为null，否则beanCopier.copyProps(jobPo, jobPo2);会有空指针异常
		jobPo.setInternalExtParam("xxx", "fadsfsa");
		jobPo.setExtParams(new HashMap<String, String>());
		jobPo.setJobType(JobType.REAL_TIME);
		jobPo.setJobDependencyPos(new ArrayList<JobDependencyPo>());
	}

	@Test
	public void testBeanCopier() {
		long start = System.currentTimeMillis();
		for (int i = 0; i < 1000000; i++) {
			JobUtils.copy(jobPo);
		}
		// 856
		System.out.println("BeanCopier cost time " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testClone() {
		long start = System.currentTimeMillis();
		for (int i = 0; i < 10000; i++) {
			BeanUtils.deepClone(jobPo);
		}
		// 50908
		System.out.println("Clone cost time " + (System.currentTimeMillis() - start));
	}

	@Test
	public void testJdkCopy() {
		AbstractCompiler.setCompiler("jdk");
		JobUtils.copy(jobPo);
	}

	@Test
	public void testJavassistCopy() {
		AbstractCompiler.setCompiler("javassist");
		JobUtils.copy(jobPo);
	}

	@Test
	public void testGetBeanCopier() throws Exception {
		BeanCopier<JobPo, JobPo> beanCopier = BeanCopierFactory.createCopier(JobPo.class, JobPo.class);

		JobPo jobPo2 = new JobPo();
		beanCopier.copyProps(jobPo, jobPo2);
		System.out.println(jobPo2);

		try {
			// source 和target 里面的类型不一致时会报异常，一个Integer，另一个是int
			// private Integer maxRetryTimes = 0;
			// private int maxRetryTimes = 0;
			Job job = new Job();
			BeanCopier<JobPo, Job> beanCopier2 = BeanCopierFactory.createCopier(JobPo.class, Job.class);
			beanCopier2.copyProps(jobPo, job);
			System.out.println(job);
		} catch (Exception e) {
		}

	}

	@Test
	public void testPropConvert() {
		try {
			// source 和target 里面的类型不一致时会报异常，一个Integer，另一个是int
			// private Integer maxRetryTimes = 0;
			// private int maxRetryTimes = 0;
			AbstractCompiler.setCompiler("javassist");
			Map<String, PropConverter<?, ?>> map = new HashMap<String, PropConverter<?, ?>>();
			map.put("taskId", new MyPropConverter());
			BeanCopier<JobPo, Job> beanCopier = BeanCopierFactory.createCopier(JobPo.class, Job.class, map);
			Job job = new Job();
			beanCopier.copyProps(jobPo, job);
			System.out.println(job);
		} catch (Exception e) {
		}

	}

	@Test
	public void testDeepCopy() {
		AbstractCompiler.setCompiler("javassist");
		BeanCopier<JobPo, JobPo> beanCopier = BeanCopierFactory.createCopier(JobPo.class, JobPo.class, true);
		JobPo jobPo2 = new JobPo();
		
		//jobPo 里面的几个list、map对象不能为null，否则beanCopier.copyProps(jobPo, jobPo2);会有空指针异常
		beanCopier.copyProps(jobPo, jobPo2);
		System.out.println(jobPo2);
	}

	public class MyPropConverter implements PropConverter<JobPo, String> {
		@Override
		public String convert(JobPo jobPo) {
			return "测试的发生的发生";
		}
	}
}
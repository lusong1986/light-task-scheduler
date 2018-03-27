package com.github.ltsopensource.core.commons.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.dianping.cat.Cat;
import com.dianping.cat.Cat.Context;
import com.dianping.cat.CatConstants;
import com.dianping.cat.message.Event;
import com.dianping.cat.message.Transaction;
import com.dianping.cat.message.spi.MessageTree;
import com.github.ltsopensource.core.logger.Logger;
import com.github.ltsopensource.core.logger.LoggerFactory;

public class CatUtils {

	private static final Logger LOGGER = LoggerFactory.getLogger(CatUtils.class);

	public static String getTraceId() {
		if (LOGGER.isInfoEnabled()) {
			MessageTree messageTree = null;
			try {
				messageTree = Cat.getManager().getThreadLocalMessageTree();
			} catch (Throwable e) {
				e.printStackTrace();
			}

			if (messageTree != null) {
				try {
					String traceId = messageTree.getRootMessageId();
					if (traceId == null) {
						traceId = Cat.getCurrentMessageId();
					}

					if (StringUtils.hasText(traceId)) {
						return traceId.replaceAll("-", StringUtils.EMPTY);
					}
				} catch (Throwable e) {
					e.printStackTrace();
				}
			}
		}

		return null;
	}

	public static void buildContext(Map<String, String> context, boolean reJoin) {
		if (context == null)
			return;
		MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
		String messageId = context.get(Cat.Context.CHILD + 1);
		String parentId = context.get(Cat.Context.PARENT);
		String rootId = context.get(Cat.Context.ROOT);
		if (!reJoin) {
			if (parentId.equals(tree.getMessageId()) || StringUtils.hasText(tree.getParentMessageId())) {
				return;
			}
		}
		if (StringUtils.hasText(messageId) && StringUtils.hasText(parentId) && StringUtils.hasText(rootId)) {
			CatUtils.buildContext(messageId, parentId, rootId);
		}
	}

	public static Map<String, String> getNewContext() {
		final Map<String, String> map = new HashMap<String, String>();
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		Runnable run = new Runnable() {
			public void run() {
				try {
					MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
					String messageId = tree.getMessageId();

					if (messageId == null) {
						messageId = Cat.createMessageId();
						tree.setMessageId(messageId);
					}

					String childId1 = Cat.createMessageId();
					Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childId1);

					String root = tree.getRootMessageId();

					if (root == null) {
						root = messageId;
					}

					map.put(Context.ROOT, root);
					map.put(Context.PARENT, messageId);
					map.put(Context.CHILD + 1, childId1);
					countDownLatch.countDown();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		};

		try {
			new Thread(run).start();
			countDownLatch.await(3000L, TimeUnit.MILLISECONDS);

			return map;
		} catch (Exception e) {
			e.printStackTrace();
		}

		return new HashMap<String, String>();
	}

	public static Map<String, String> getContext() {
		if (LOGGER.isInfoEnabled()) {
			try {
				MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
				String messageId = tree.getMessageId();

				if (messageId == null) {
					messageId = Cat.createMessageId();
					tree.setMessageId(messageId);
				}

				String childId1 = Cat.createMessageId();
				Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childId1);

				String root = tree.getRootMessageId();

				if (root == null) {
					root = messageId;
				}

				Map<String, String> map = new HashMap<String, String>();
				map.put(Context.ROOT, root);
				map.put(Context.PARENT, messageId);
				map.put(Context.CHILD + 1, childId1);

				return map;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		return new HashMap<String, String>();
	}

	public static void buildContext(String messageId, String parentId, String rootId) {
		if (LOGGER.isInfoEnabled()) {
			try {
				MessageTree tree = Cat.getManager().getThreadLocalMessageTree();

				if (messageId != null) {
					tree.setMessageId(messageId);
				}
				if (parentId != null) {
					tree.setParentMessageId(parentId);
				}
				if (rootId != null) {
					tree.setRootMessageId(rootId);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void catLog(Throwable e) {
		if (LOGGER.isInfoEnabled()) {
			try {
				Cat.logError(e);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_INVOKE_LOG", throwable);
			}
		}
	}

	public static void catSuccess(Transaction transaction) {
		if (LOGGER.isInfoEnabled()) {
			if (transaction == null) {
				return;
			}

			try {
				transaction.setStatus(Transaction.SUCCESS);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_SET_SUCCESS", throwable);
			}
		}
	}

	public static void catException(Transaction transaction, Throwable e) {
		if (LOGGER.isInfoEnabled()) {

			if (transaction == null) {
				return;
			}

			try {
				transaction.setStatus(e);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_SET_EXCEPTION", throwable);
			}
		}
	}

	public static void catComplete(Transaction transaction) {
		if (LOGGER.isInfoEnabled()) {
			if (transaction == null) {
				return;
			}

			try {
				transaction.complete();
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_SET_COMPLETE", throwable);
			}
		}
	}

	public static void catEvent(String type, String name) {
		if (LOGGER.isInfoEnabled()) {
			try {
				Cat.logEvent(type, name);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_INVOKE_EVENT", throwable);
			}
		}
	}

	public static void catEvent(String type, String name, String status, String namePairs) {
		if (LOGGER.isInfoEnabled()) {
			try {
				Cat.logEvent(type, name, status, namePairs);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_INVOKE_EVENT", throwable);
			}
		}
	}

	public static void recordEvent(final boolean eventStatus, final String jobName, final String opType) {
		String status = "1";// failed
		if (eventStatus) {
			status = Event.SUCCESS;
		}

		catEvent(opType, jobName, status, null);
	}

	public static void catMetric(String name, int count) {
		if (LOGGER.isInfoEnabled()) {
			try {
				Cat.logMetricForCount(name, count);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_INVOKE_EVENT", throwable);
			}
		}
	}

	public static Transaction catTransaction(String type, String name) {
		if (LOGGER.isInfoEnabled()) {
			try {
				return Cat.newTransaction(type, name);
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_CREATE_TRANS", throwable);
			}
		}
		return null;
	}

	public static Transaction peekTransaction() {
		if (LOGGER.isInfoEnabled()) {
			try {
				return Cat.getManager().getPeekTransaction();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return null;
	}

	public static void catException(Transaction transaction) {
		if (LOGGER.isInfoEnabled()) {

			if (transaction == null) {
				return;
			}

			try {
				transaction.setStatus("Exception");
			} catch (Throwable throwable) {
				LOGGER.error("LOG_FAILED_TO_SET_EXCEPTION", throwable);
			}
		}
	}

	public static void setMessageId(String messageId) {
		if (LOGGER.isInfoEnabled()) {
			if (StringUtils.isEmpty(messageId)) {
				return;
			}
			try {
				Cat.getManager().getThreadLocalMessageTree().setMessageId(messageId);
			} catch (Exception e) {
			}
		}
	}
}

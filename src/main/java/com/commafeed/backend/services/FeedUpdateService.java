package com.commafeed.backend.services;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.commafeed.backend.MetricsBean;
import com.commafeed.backend.cache.CacheService;
import com.commafeed.backend.dao.FeedEntryDAO;
import com.commafeed.backend.dao.FeedEntryStatusDAO;
import com.commafeed.backend.dao.FeedSubscriptionDAO;
import com.commafeed.backend.feeds.FeedRefreshUpdater;
import com.commafeed.backend.model.Feed;
import com.commafeed.backend.model.FeedEntry;
import com.commafeed.backend.model.FeedEntryContent;
import com.commafeed.backend.model.FeedSubscription;
import com.commafeed.backend.model.User;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Striped;

@Stateless
public class FeedUpdateService {

	protected static Logger log = LoggerFactory.getLogger(FeedRefreshUpdater.class);

	private static Striped<Lock> LOCKS = Striped.lazyWeakLock(100000);

	@PersistenceContext
	protected EntityManager em;

	@Inject
	FeedSubscriptionDAO feedSubscriptionDAO;

	@Inject
	FeedEntryDAO feedEntryDAO;

	@Inject
	FeedEntryStatusDAO feedEntryStatusDAO;

	@Inject
	MetricsBean metricsBean;

	@Inject
	CacheService cache;

	@Inject
	FeedEntryContentService feedEntryContentService;

	public boolean addEntries(final Feed feed, final List<FeedEntry> entries, final List<FeedSubscription> subscriptions) {
		List<String> keys = Lists.newArrayList();
		for (FeedEntry entry : entries) {
			// lock on feed, make sure we are not updating the same feed twice at the same time
			keys.add(StringUtils.trimToEmpty("" + feed.getId()));

			// lock on content, make sure we are not updating the same entry twice at the same time
			FeedEntryContent content = entry.getContent();
			keys.add(DigestUtils.sha1Hex(StringUtils.trimToEmpty(content.getContent() + content.getTitle())));
		}

		List<Lock> locks = Lists.newArrayList();
		boolean locked = true;
		try {
			for (Lock lock : LOCKS.bulkGet(keys)) {
				if (lock.tryLock(1, TimeUnit.MINUTES)) {
					locks.add(lock);
				} else {
					locked = false;
					break;
				}
			}

			if (locked) {
				for (FeedEntry entry : entries) {
					addEntry(feed, entry);
				}
			} else {
				log.error("lock timeout for {}", feed.getUrl());
			}
		} catch (InterruptedException e) {
			log.error("interrupted while waiting for lock for " + feed.getUrl() + " : " + e.getMessage(), e);
		} finally {
			for (Lock lock : locks) {
				lock.unlock();
			}
		}

		List<User> users = Lists.newArrayList();
		for (FeedSubscription sub : subscriptions) {
			users.add(sub.getUser());
		}
		cache.invalidateUnreadCount(subscriptions.toArray(new FeedSubscription[0]));
		cache.invalidateUserRootCategory(users.toArray(new User[0]));

		return locked;
	}

	/**
	 * thread-unsafe insertion
	 */
	private void addEntry(Feed feed, FeedEntry entry) {

		Long existing = feedEntryDAO.findExisting(entry.getGuid(), feed.getId());
		if (existing != null) {
			return;
		}

		FeedEntryContent content = feedEntryContentService.findOrCreate(entry.getContent(), feed.getLink());
		entry.setGuidHash(DigestUtils.sha1Hex(entry.getGuid()));
		entry.setContent(content);
		entry.setInserted(new Date());
		entry.setFeed(feed);

		feedEntryDAO.saveOrUpdate(entry);
		metricsBean.entryInserted();
	}
}

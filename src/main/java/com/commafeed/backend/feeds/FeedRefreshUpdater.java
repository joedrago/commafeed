package com.commafeed.backend.feeds;

import java.util.Collection;
import java.util.Date;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.commafeed.backend.MetricsBean;
import com.commafeed.backend.cache.CacheService;
import com.commafeed.backend.dao.FeedDAO;
import com.commafeed.backend.dao.FeedEntryDAO;
import com.commafeed.backend.dao.FeedSubscriptionDAO;
import com.commafeed.backend.feeds.FeedRefreshExecutor.Task;
import com.commafeed.backend.model.ApplicationSettings;
import com.commafeed.backend.model.Feed;
import com.commafeed.backend.model.FeedEntry;
import com.commafeed.backend.model.FeedSubscription;
import com.commafeed.backend.pubsubhubbub.SubscriptionHandler;
import com.commafeed.backend.services.ApplicationSettingsService;
import com.commafeed.backend.services.FeedUpdateService;
import com.google.api.client.util.Lists;

@ApplicationScoped
public class FeedRefreshUpdater {

	protected static Logger log = LoggerFactory.getLogger(FeedRefreshUpdater.class);

	@Inject
	FeedUpdateService feedUpdateService;

	@Inject
	SubscriptionHandler handler;

	@Inject
	FeedRefreshTaskGiver taskGiver;

	@Inject
	FeedDAO feedDAO;

	@Inject
	ApplicationSettingsService applicationSettingsService;

	@Inject
	MetricsBean metricsBean;

	@Inject
	FeedSubscriptionDAO feedSubscriptionDAO;

	@Inject
	FeedEntryDAO feedEntryDAO;

	@Inject
	CacheService cache;

	private FeedRefreshExecutor pool;

	@PostConstruct
	public void init() {
		ApplicationSettings settings = applicationSettingsService.get();
		int threads = Math.max(settings.getDatabaseUpdateThreads(), 1);
		pool = new FeedRefreshExecutor("feed-refresh-updater", threads, Math.min(50 * threads, 1000));
	}

	@PreDestroy
	public void shutdown() {
		pool.shutdown();
	}

	public void updateFeed(Feed feed, Collection<FeedEntry> entries) {
		pool.execute(new EntryTask(feed, entries));
	}

	private class EntryTask implements Task {

		private Feed feed;
		private Collection<FeedEntry> entries;

		public EntryTask(Feed feed, Collection<FeedEntry> entries) {
			this.feed = feed;
			this.entries = entries;
		}

		@Override
		public void run() {
			boolean ok = true;
			if (!entries.isEmpty()) {

				List<String> cachedEntries = cache.getLastEntries(feed);
				List<String> newCachedEntries = Lists.newArrayList();
				List<FeedEntry> newEntries = Lists.newArrayList();
				for (FeedEntry entry : entries) {
					String cacheKey = cache.buildUniqueEntryKey(feed, entry);
					if (!cachedEntries.contains(cacheKey)) {
						log.debug("cache miss for {}", entry.getUrl());
						newEntries.add(entry);
						metricsBean.entryCacheMiss();
					} else {
						log.debug("cache hit for {}", entry.getUrl());
						metricsBean.entryCacheHit();
					}
					newCachedEntries.add(cacheKey);
				}
				if (!newEntries.isEmpty()) {
					List<FeedSubscription> subscriptions = feedSubscriptionDAO.findByFeed(feed);
					try {
						ok = feedUpdateService.addEntries(feed, newEntries, subscriptions);
					} catch (Exception e) {
						log.error(e.getMessage(), e);
						ok = false;
					}
				}
				cache.setLastEntries(feed, newCachedEntries);
			}

			if (applicationSettingsService.get().isPubsubhubbub()) {
				handlePubSub(feed);
			}
			if (!ok) {
				// requeue asap
				feed.setDisabledUntil(new Date(0));
			}
			metricsBean.feedUpdated();
			taskGiver.giveBack(feed);
		}

		@Override
		public boolean isUrgent() {
			return feed.isUrgent();
		}
	}

	private void handlePubSub(final Feed feed) {
		if (feed.getPushHub() != null && feed.getPushTopic() != null) {
			Date lastPing = feed.getPushLastPing();
			Date now = new Date();
			if (lastPing == null || lastPing.before(DateUtils.addDays(now, -3))) {
				new Thread() {
					@Override
					public void run() {
						handler.subscribe(feed);
					}
				}.start();
			}
		}
	}

	public int getQueueSize() {
		return pool.getQueueSize();
	}

	public int getActiveCount() {
		return pool.getActiveCount();
	}

}

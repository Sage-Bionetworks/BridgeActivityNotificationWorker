package org.sagebionetworks.bridge.notification.helper;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Resource;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.collect.ImmutableSet;
import com.jcabi.aspects.Cacheable;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.notification.worker.WorkerConfig;

@Component("NotificationWorkerDynamoHelper")
public class DynamoHelper {
    static final String KEY_BURST_DURATION_DAYS = "burstDurationDays";
    static final String KEY_BURST_EVENT_ID_SET = "burstStartEventIdSet";
    static final String KEY_BURST_TASK_ID = "burstTaskId";
    static final String KEY_EXCLUDED_DATA_GROUP_SET = "excludedDataGroupSet";
    static final String KEY_FINISH_TIME = "finishTime";
    static final String KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START = "notificationBlackoutDaysFromStart";
    static final String KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END = "notificationBlackoutDaysFromEnd";
    static final String KEY_NOTIFICATION_MESSAGE = "notificationMessage";
    static final String KEY_NOTIFICATION_TIME = "notificationTime";
    static final String KEY_NUM_MISSED_DAYS_TO_NOTIFY = "numMissedDaysToNotify";
    static final String KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY = "numMissedConsecutiveDaysToNotify";
    static final String KEY_REQUIRED_SUBPOPULATION_GUID_SET = "requiredSubpopulationGuidSet";
    static final String KEY_STUDY_ID = "studyId";
    static final String KEY_TAG = "tag";
    static final String KEY_USER_ID = "userId";
    static final String KEY_WORKER_ID = "workerId";
    static final String VALUE_WORKER_ID = "ActivityNotificationWorker";

    private Table ddbNotificationConfigTable;
    private Table ddbNotificationLogTable;
    private Table ddbWorkerLogTable;
    private DynamoQueryHelper dynamoQueryHelper;

    @Resource(name = "ddbNotificationConfigTable")
    public final void setDdbNotificationConfigTable(Table ddbNotificationConfigTable) {
        this.ddbNotificationConfigTable = ddbNotificationConfigTable;
    }

    @Resource(name = "ddbNotificationLogTable")
    public final void setDdbNotificationLogTable(Table ddbNotificationLogTable) {
        this.ddbNotificationLogTable = ddbNotificationLogTable;
    }

    @Resource(name = "ddbWorkerLogTable")
    public final void setDdbWorkerLogTable(Table ddbWorkerLogTable) {
        this.ddbWorkerLogTable = ddbWorkerLogTable;
    }

    @Autowired
    public final void setDynamoQueryHelper(DynamoQueryHelper dynamoQueryHelper) {
        this.dynamoQueryHelper = dynamoQueryHelper;
    }

    @Cacheable(lifetime = 5, unit = TimeUnit.MINUTES)
    public WorkerConfig getNotificationConfigForStudy(String studyId) {
        Item item = ddbNotificationConfigTable.getItem(KEY_STUDY_ID, studyId);
        WorkerConfig workerConfig = new WorkerConfig();
        workerConfig.setBurstDurationDays(item.getInt(KEY_BURST_DURATION_DAYS));
        workerConfig.setBurstStartEventIdSet(getNonNullStringSet(item, KEY_BURST_EVENT_ID_SET));
        workerConfig.setBurstTaskId(item.getString(KEY_BURST_TASK_ID));
        workerConfig.setExcludedDataGroupSet(getNonNullStringSet(item, KEY_EXCLUDED_DATA_GROUP_SET));
        workerConfig.setNotificationBlackoutDaysFromStart(item.getInt(KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START));
        workerConfig.setNotificationBlackoutDaysFromEnd(item.getInt(KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END));
        workerConfig.setNotificationMessage(item.getString(KEY_NOTIFICATION_MESSAGE));
        workerConfig.setNumMissedConsecutiveDaysToNotify(item.getInt(KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY));
        workerConfig.setNumMissedDaysToNotify(item.getInt(KEY_NUM_MISSED_DAYS_TO_NOTIFY));
        workerConfig.setRequiredSubpopulationGuidSet(getNonNullStringSet(item, KEY_REQUIRED_SUBPOPULATION_GUID_SET));
        return workerConfig;
    }

    public Long getLastNotificationTimeForUser(String userId) {
        // To get the latest notification time, sort the index in reverse and limit the result set to 1.
        QuerySpec query = new QuerySpec().withHashKey(KEY_USER_ID, userId).withScanIndexForward(false)
                .withMaxResultSize(1);
        Iterator<Item> itemIter = dynamoQueryHelper.query(ddbNotificationLogTable, query).iterator();
        if (itemIter.hasNext()) {
            Item item = itemIter.next();
            return item.getLong(KEY_NOTIFICATION_TIME);
        } else {
            return null;
        }
    }

    public void setLastNotificationTimeForUser(String userId, long notificationTime) {
        Item item = new Item().withPrimaryKey(KEY_USER_ID, userId, KEY_NOTIFICATION_TIME, notificationTime);
        ddbNotificationLogTable.putItem(item);
    }

    public void writeWorkerLog(String tag) {
        Item item = new Item().withPrimaryKey(KEY_WORKER_ID, VALUE_WORKER_ID, KEY_FINISH_TIME,
                DateTime.now().getMillis()).withString(KEY_TAG, tag);
        ddbWorkerLogTable.putItem(item);
    }

    // In DDB, empty sets are not allowed. In Java, we prefer empty sets over nulls. Null check and convert nulls to
    // empty sets.
    private Set<String> getNonNullStringSet(Item item, String key) {
        if (item.hasAttribute(key)) {
            return item.getStringSet(key);
        } else {
            return ImmutableSet.of();
        }
    }
}

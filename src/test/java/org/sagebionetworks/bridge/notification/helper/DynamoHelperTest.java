package org.sagebionetworks.bridge.notification.helper;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import org.sagebionetworks.bridge.dynamodb.DynamoQueryHelper;
import org.sagebionetworks.bridge.notification.worker.WorkerConfig;

public class DynamoHelperTest {
    private static final long MOCK_NOW_MILLIS = DateTime.parse("2018-04-27T16:41:15.831-0700").getMillis();
    private static final String STUDY_ID = "test-study";
    private static final String USER_ID = "test-user";

    private DynamoHelper dynamoHelper;
    private DynamoQueryHelper mockQueryHelper;
    private Table mockNotificationConfigTable;
    private Table mockNotificationLogTable;
    private Table mockWorkerLogTable;

    @BeforeClass
    public static void mockNow() {
        DateTimeUtils.setCurrentMillisFixed(MOCK_NOW_MILLIS);
    }

    @AfterClass
    public static void unmockNow() {
        DateTimeUtils.setCurrentMillisSystem();
    }

    @BeforeMethod
    public void before() {
        // Set up mocks
        mockQueryHelper = mock(DynamoQueryHelper.class);
        mockNotificationConfigTable = mock(Table.class);
        mockNotificationLogTable = mock(Table.class);
        mockWorkerLogTable = mock(Table.class);

        // Create DynamoHelper
        dynamoHelper = new DynamoHelper();
        dynamoHelper.setDynamoQueryHelper(mockQueryHelper);
        dynamoHelper.setDdbNotificationConfigTable(mockNotificationConfigTable);
        dynamoHelper.setDdbNotificationLogTable(mockNotificationLogTable);
        dynamoHelper.setDdbWorkerLogTable(mockWorkerLogTable);
    }

    @Test
    public void getNotificationConfigForStudy() {
        // Set up mock
        Item item = new Item()
                .withPrimaryKey(DynamoHelper.KEY_STUDY_ID, STUDY_ID)
                .withInt(DynamoHelper.KEY_BURST_DURATION_DAYS, 19)
                .withStringSet(DynamoHelper.KEY_BURST_EVENT_ID_SET, "enrollment", "custom:activityBurst2Start")
                .withString(DynamoHelper.KEY_BURST_TASK_ID, "study-burst-task")
                .withStringSet(DynamoHelper.KEY_EXCLUDED_DATA_GROUP_SET, "excluded-group-1", "excluded-group-2")
                .withInt(DynamoHelper.KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_START, 2)
                .withInt(DynamoHelper.KEY_NOTIFICATION_BLACKOUT_DAYS_FROM_END, 1)
                .withString(DynamoHelper.KEY_NOTIFICATION_MESSAGE, "dummy message")
                .withInt(DynamoHelper.KEY_NUM_MISSED_CONSECUTIVE_DAYS_TO_NOTIFY, 3)
                .withInt(DynamoHelper.KEY_NUM_MISSED_DAYS_TO_NOTIFY, 4)
                .withStringSet(DynamoHelper.KEY_REQUIRED_SUBPOPULATION_GUID_SET, STUDY_ID);
        when(mockNotificationConfigTable.getItem(DynamoHelper.KEY_STUDY_ID, STUDY_ID)).thenReturn(item);

        // Execute and validate
        WorkerConfig config = dynamoHelper.getNotificationConfigForStudy(STUDY_ID);
        assertEquals(config.getBurstDurationDays(), 19);
        assertEquals(config.getBurstStartEventIdSet(), ImmutableSet.of("enrollment",
                "custom:activityBurst2Start"));
        assertEquals(config.getBurstTaskId(), "study-burst-task");
        assertEquals(config.getExcludedDataGroupSet(), ImmutableSet.of("excluded-group-1",
                "excluded-group-2"));
        assertEquals(config.getNotificationBlackoutDaysFromStart(), 2);
        assertEquals(config.getNotificationBlackoutDaysFromEnd(), 1);
        assertEquals(config.getNotificationMessage(), "dummy message");
        assertEquals(config.getNumMissedConsecutiveDaysToNotify(), 3);
        assertEquals(config.getNumMissedDaysToNotify(), 4);
        assertEquals(config.getRequiredSubpopulationGuidSet(), ImmutableSet.of(STUDY_ID));

        verify(mockNotificationConfigTable).getItem(DynamoHelper.KEY_STUDY_ID, STUDY_ID);

        // Test caching
        WorkerConfig config2 = dynamoHelper.getNotificationConfigForStudy(STUDY_ID);
        assertNotNull(config2);
        verifyNoMoreInteractions(mockNotificationConfigTable);
    }

    @Test
    public void getLastNotificationTimeForUser_NormalCase() {
        // Set up mock
        Item item = new Item().withPrimaryKey(DynamoHelper.KEY_USER_ID, USER_ID, DynamoHelper.KEY_NOTIFICATION_TIME,
                1234L);
        when(mockQueryHelper.query(same(mockNotificationLogTable), any())).thenReturn(ImmutableList.of(item));

        // Execute and validate
        Long result = dynamoHelper.getLastNotificationTimeForUser(USER_ID);
        assertEquals(result.longValue(), 1234L);

        ArgumentCaptor<QuerySpec> queryCaptor = ArgumentCaptor.forClass(QuerySpec.class);
        verify(mockQueryHelper).query(same(mockNotificationLogTable), queryCaptor.capture());

        QuerySpec query = queryCaptor.getValue();
        assertEquals(query.getHashKey().getName(), DynamoHelper.KEY_USER_ID);
        assertEquals(query.getHashKey().getValue(), USER_ID);
        assertFalse(query.isScanIndexForward());
        assertEquals(query.getMaxResultSize().intValue(), 1);
    }

    @Test
    public void getLastNotificationTimeForUser_NoResult() {
        // Set up mock
        when(mockQueryHelper.query(same(mockNotificationLogTable), any())).thenReturn(ImmutableList.of());

        // Execute and validate
        Long result = dynamoHelper.getLastNotificationTimeForUser(USER_ID);
        assertNull(result);
    }

    @Test
    public void setLastNotificationTimeForUser() {
        // Execute
        dynamoHelper.setLastNotificationTimeForUser(USER_ID, 1234L);

        // Validate back-end
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockNotificationLogTable).putItem(itemCaptor.capture());

        Item item = itemCaptor.getValue();
        assertEquals(item.getString(DynamoHelper.KEY_USER_ID), USER_ID);
        assertEquals(item.getLong(DynamoHelper.KEY_NOTIFICATION_TIME), 1234L);
    }

    @Test
    public void writeWorkerLog() {
        // Execute
        dynamoHelper.writeWorkerLog("dummy tag");

        // Validate back-end
        ArgumentCaptor<Item> itemCaptor = ArgumentCaptor.forClass(Item.class);
        verify(mockWorkerLogTable).putItem(itemCaptor.capture());

        Item item = itemCaptor.getValue();
        assertEquals(item.getString(DynamoHelper.KEY_WORKER_ID), DynamoHelper.VALUE_WORKER_ID);
        assertEquals(item.getLong(DynamoHelper.KEY_FINISH_TIME), MOCK_NOW_MILLIS);
        assertEquals(item.getString(DynamoHelper.KEY_TAG), "dummy tag");
    }
}

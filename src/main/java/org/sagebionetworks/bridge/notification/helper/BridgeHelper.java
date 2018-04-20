package org.sagebionetworks.bridge.notification.helper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.rest.ClientManager;
import org.sagebionetworks.bridge.rest.api.ForWorkersApi;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.SmsTemplate;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;

@Component("NotificationWorkerBridgeHelper")
public class BridgeHelper {
    private ClientManager clientManager;

    /** Bridge client manager. */
    @Autowired
    public final void setClientManager(ClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public List<ActivityEvent> getActivityEvents(String studyId, String userId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getActivityEventsForParticipant(studyId, userId).execute()
                .body().getItems();
    }

    public Iterator<AccountSummary> getAllAccountSummaries(String studyId) {
        return new AccountSummaryIterator(clientManager, studyId);
    }

    public StudyParticipant getParticipant(String studyId, String userId) throws IOException {
        return clientManager.getClient(ForWorkersApi.class).getParticipantById(studyId, userId, true)
                .execute().body();
    }

    public Iterator<ScheduledActivity> getTaskHistory(String studyId, String userId, String taskId,
            DateTime scheduledOnStart, DateTime scheduledOnEnd) {
        return new TaskHistoryIterator(clientManager, studyId, userId, taskId, scheduledOnStart, scheduledOnEnd);
    }

    public void sendSmsToUser(String studyId, String userId, String message) throws IOException {
        SmsTemplate smsTemplate = new SmsTemplate().message(message);
        clientManager.getClient(ForWorkersApi.class).sendSmsMessageToParticipant(studyId, userId, smsTemplate)
                .execute();
    }
}

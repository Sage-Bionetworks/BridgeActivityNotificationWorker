package org.sagebionetworks.bridge.notification.worker;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.RateLimiter;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import org.sagebionetworks.bridge.notification.helper.BridgeHelper;
import org.sagebionetworks.bridge.notification.helper.DynamoHelper;
import org.sagebionetworks.bridge.rest.model.AccountSummary;
import org.sagebionetworks.bridge.rest.model.ActivityEvent;
import org.sagebionetworks.bridge.rest.model.ScheduleStatus;
import org.sagebionetworks.bridge.rest.model.ScheduledActivity;
import org.sagebionetworks.bridge.rest.model.StudyParticipant;
import org.sagebionetworks.bridge.rest.model.UserConsentHistory;
import org.sagebionetworks.bridge.sqs.PollSqsWorkerBadRequestException;
import org.sagebionetworks.bridge.time.DateUtils;
import org.sagebionetworks.bridge.worker.ThrowingConsumer;

// todo doc
@Component("ActivityNotificationWorker")
public class BridgeNotificationWorkerProcessor implements ThrowingConsumer<JsonNode> {
    private static final Logger LOG = LoggerFactory.getLogger(BridgeNotificationWorkerProcessor.class);

    private static final int REPORTING_INTERVAL = 250;

    static final String REQUEST_PARAM_DATE = "date";
    static final String REQUEST_PARAM_STUDY_ID = "studyId";
    static final String REQUEST_PARAM_TAG = "tag";

    private final RateLimiter perUserRateLimiter = RateLimiter.create(1.0);

    private BridgeHelper bridgeHelper;
    private DynamoHelper dynamoHelper;

    @Autowired
    public final void setBridgeHelper(BridgeHelper bridgeHelper) {
        this.bridgeHelper = bridgeHelper;
    }

    @Autowired
    public final void setDynamoHelper(DynamoHelper dynamoHelper) {
        this.dynamoHelper = dynamoHelper;
    }

    /** Set rate limit, in users per second. */
    public final void setPerUserRateLimit(double rate) {
        perUserRateLimiter.setRate(rate);
    }

    @Override
    public void accept(JsonNode jsonNode) throws PollSqsWorkerBadRequestException {
        // Get request args
        // studyId
        JsonNode studyIdNode = jsonNode.get(REQUEST_PARAM_STUDY_ID);
        if (studyIdNode == null || studyIdNode.isNull()) {
            throw new PollSqsWorkerBadRequestException("studyId must be specified");
        }
        String studyId = studyIdNode.textValue();

        // date
        JsonNode dateNode = jsonNode.get(REQUEST_PARAM_DATE);
        if (dateNode == null || dateNode.isNull()) {
            throw new PollSqsWorkerBadRequestException("date must be specified");
        }

        String dateString = dateNode.textValue();
        LocalDate date;
        try {
            date = LocalDate.parse(dateString);
        } catch (IllegalArgumentException | NullPointerException ex) {
            throw new PollSqsWorkerBadRequestException("date must be in the format YYYY-MM-DD");
        }

        // tag
        JsonNode tagNode = jsonNode.get(REQUEST_PARAM_TAG);
        String tag = null;
        if (tagNode != null && !tagNode.isNull()) {
            tag = tagNode.textValue();
        }

        LOG.info("Received request for study=" + studyId + ", date=" + dateString + ", tag=" + tag);

        // Iterate over each user
        Iterator<AccountSummary> accountSummaryIterator = bridgeHelper.getAllAccountSummaries(studyId);
        int numUsers = 0;
        Stopwatch stopwatch = Stopwatch.createStarted();
        while (accountSummaryIterator.hasNext()) {
            // Rate limit
            perUserRateLimiter.acquire();

            // Process
            try {
                AccountSummary oneAccountSummary = accountSummaryIterator.next();

                try {
                    processAccountForDate(studyId, date, oneAccountSummary);
                } catch (Exception ex) {
                    LOG.error("Error processing user ID " + oneAccountSummary.getId() + ": " + ex.getMessage(), ex);
                }
            } catch (Exception ex) {
                LOG.error("Error getting next user: " + ex.getMessage(), ex);
            }

            // Reporting
            numUsers++;
            if (numUsers % REPORTING_INTERVAL == 0) {
                LOG.info("Processing users in progress: " + numUsers + " users in " +
                        stopwatch.elapsed(TimeUnit.SECONDS) + " seconds");
            }
        }

        // Write to Worker Log in DDB so we can signal end of processing.
        dynamoHelper.writeWorkerLog(tag);

        LOG.info("Finished processing users: " + numUsers + " users in " + stopwatch.elapsed(TimeUnit.SECONDS) +
                " seconds");
        LOG.info("Finished processing request for study " + studyId + " and date " + dateString);
    }

    private void processAccountForDate(String studyId, LocalDate date, AccountSummary accountSummary)
            throws IOException {
        // Get participant. We'll need some attributes.
        StudyParticipant participant = bridgeHelper.getParticipant(studyId, accountSummary.getId());

        // Exclude users who are not eligible for notifications.
        if (shouldExcludeUser(studyId, participant)) {
            return;
        }

        // Get the current activity burst.
        ActivityEvent burstEvent = getCurrentActivityBurstEventForParticipant(studyId, date, participant);
        if (burstEvent == null) {
             // We're not currently in an activity burst. (Or we are, but we're in the blackout period.) Skip
            // processing this user.
            return;
        }

        // Determine if we need to notify the user.
        if (shouldSendNotificationToUser(studyId, date, participant, burstEvent)) {
            notifyUser(studyId, participant);
        }
    }

    private boolean shouldExcludeUser(String studyId, StudyParticipant participant) {
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        Set<String> excludedDataGroupSet = workerConfig.getExcludedDataGroupSet();

        // Unverified phone numbers can't be notified
        if (Boolean.FALSE.equals(participant.getPhoneVerified())) {
            return true;
        }

        // Users without timezones can't be processed
        if (participant.getTimeZone() == null) {
            return true;
        }

        // Unconsented users can't be notified
        if (!isUserConsented(studyId, participant)) {
            return true;
        }

        // If the user has any of the excluded data groups, exclude the user
        for (String oneUserDataGroup : participant.getDataGroups()) {
            if (excludedDataGroupSet.contains(oneUserDataGroup)) {
                return true;
            }
        }

        // If user was already sent a notification in the last burst duration, don't send another one
        Long notificationTime = dynamoHelper.getLastNotificationTimeForUser(participant.getId());
        if (notificationTime != null && notificationTime > DateTime.now()
                .minusDays(workerConfig.getBurstDurationDays()).getMillis()) {
            return true;
        }

        // We've checked all the exclude conditions. Do not exclude user.
        return false;
    }

    private boolean isUserConsented(String studyId, StudyParticipant participant) {
        // Check each required subpop. The user must have signed that consent.
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        Map<String, List<UserConsentHistory>> consentsBySubpop = participant.getConsentHistories();
        for (String oneRequiredSubpopGuid : workerConfig.getRequiredSubpopulationGuidSet()) {
            List<UserConsentHistory> oneConsentList = consentsBySubpop.get(oneRequiredSubpopGuid);
            if (oneConsentList == null || oneConsentList.isEmpty()) {
                return false;
            }

            // Newest consent is always at the end.
            UserConsentHistory newestConsent = oneConsentList.get(oneConsentList.size() - 1);
            if (!newestConsent.getHasSignedActiveConsent() || newestConsent.getWithdrewOn() != null) {
                return false;
            }
        }

        // If we make it this far, then we've verified that all consents are signed, up-to-date, and not withdrawn.
        return true;
    }

    private ActivityEvent getCurrentActivityBurstEventForParticipant(String studyId, LocalDate date,
            StudyParticipant participant) throws IOException {
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        List<ActivityEvent> activityEventList = bridgeHelper.getActivityEvents(studyId, participant.getId());

        for (ActivityEvent oneActivityEvent : activityEventList) {
            // Skip if this event isn't a burst start.
            if (!workerConfig.getBurstStartEventIdSet().contains(oneActivityEvent.getEventId())) {
                continue;
            }

            // Calculate burst bounds. End date is start + period - 1. Skip if the current day is not within the burst
            // period (inclusive).
            DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString(participant.getTimeZone());
            LocalDate burstStartDate = oneActivityEvent.getTimestamp().withZone(timeZone).toLocalDate();
            LocalDate burstEndDate = burstStartDate.plusDays(workerConfig.getBurstDurationDays()).minusDays(1);
            if (date.isBefore(burstStartDate) || date.isAfter(burstEndDate)) {
                continue;
            }

            // We found the current activity burst. Activity bursts do not overlap, so we don't need to look at any
            // other activity events. However, we want to check the notification blackout periods. If we're still
            // within the blackout period, return null.
            LocalDate notificationStartDate = burstStartDate.plusDays(workerConfig
                    .getNotificationBlackoutDaysFromStart());
            LocalDate notificationEndDate = burstEndDate.minusDays(workerConfig.getNotificationBlackoutDaysFromEnd());
            if (date.isBefore(notificationStartDate) || date.isAfter(notificationEndDate)) {
                return null;
            }

            // We should process based on this activity event.
            return oneActivityEvent;
        }

        // We checked all the activity events, and we've determined we're not currently in an activity burst.
        return null;
    }

    private boolean shouldSendNotificationToUser(String studyId, LocalDate date, StudyParticipant participant,
            ActivityEvent burstEvent) {
        String userId = participant.getId();

        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);
        String taskId = workerConfig.getBurstTaskId();
        int numMissedDaysToNotify = workerConfig.getNumMissedDaysToNotify();
        int numMissedConsecutiveDaysToNotify = workerConfig.getNumMissedConsecutiveDaysToNotify();

        // Get user's activities between the burst start and now, including today's activities. Note that because of
        // how scheduling works, we might have tasks scheduled on midnight before the start of the activity burst.
        DateTimeZone timeZone = DateUtils.parseZoneFromOffsetString(participant.getTimeZone());
        LocalDate burstStartDate = burstEvent.getTimestamp().withZone(timeZone).toLocalDate();
        DateTime activityRangeStart = burstStartDate.toDateTimeAtStartOfDay();
        DateTime activityRangeEnd = date.plusDays(1).toDateTimeAtStartOfDay(timeZone);
        Iterator<ScheduledActivity> activityIterator = bridgeHelper.getTaskHistory(studyId, userId, taskId,
                activityRangeStart, activityRangeEnd);

        // If the user somehow has no activities with this task ID, don't notify the user. The account is probably not
        // fully bootstrapped, and we should avoid sending them a notification.
        if (!activityIterator.hasNext()) {
            return false;
        }

        // Map the events by scheduled date so it's easier to work with.
        Map<LocalDate, ScheduledActivity> activitiesByDate = new HashMap<>();
        while (activityIterator.hasNext()) {
            ScheduledActivity oneActivity = activityIterator.next();
            LocalDate scheduleDate = oneActivity.getScheduledOn().withZone(timeZone).toLocalDate();
            if (activitiesByDate.containsKey(scheduleDate)) {
                // This shouldn't happen. If it does, log a warning and move on.
                LOG.warn("Duplicate activities found for userId=" + userId + ", taskId=" + taskId + ", date=" +
                        scheduleDate);
            } else {
                activitiesByDate.put(scheduleDate, oneActivity);
            }
        }

        // Check today's activities first. If they did today's activities, don't bother notifying.
        ScheduledActivity todaysActivity = activitiesByDate.get(date);
        if (todaysActivity != null && todaysActivity.getStatus() == ScheduleStatus.FINISHED) {
            return false;
        }

        // Loop through activities in order by date
        int daysMissed = 0;
        int consecutiveDaysMissed = 0;
        for (LocalDate d = burstStartDate; !d.isAfter(date); d = d.plusDays(1)) {
            ScheduledActivity daysActivity = activitiesByDate.get(d);
            if (daysActivity == null || daysActivity.getStatus() != ScheduleStatus.FINISHED) {
                daysMissed++;
                consecutiveDaysMissed++;

                if (daysMissed >= numMissedDaysToNotify) {
                    return true;
                }
                if (consecutiveDaysMissed >= numMissedConsecutiveDaysToNotify) {
                    return true;
                }
            } else {
                consecutiveDaysMissed = 0;
            }
        }

        // If we make it this far, we've determined we don't need to notify.
        return false;
    }

    private void notifyUser(String studyId, StudyParticipant participant) throws IOException {
        String userId = participant.getId();
        WorkerConfig workerConfig = dynamoHelper.getNotificationConfigForStudy(studyId);

        LOG.info("Sending notification to user " + userId);

        // Log in Dynamo that we notified this user
        dynamoHelper.setLastNotificationTimeForUser(userId, DateUtils.getCurrentMillisFromEpoch());

        // Send SMS
        bridgeHelper.sendSmsToUser(studyId, userId, workerConfig.getNotificationMessage());
    }
}

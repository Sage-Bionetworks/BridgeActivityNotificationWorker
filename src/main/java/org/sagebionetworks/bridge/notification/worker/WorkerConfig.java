package org.sagebionetworks.bridge.notification.worker;

import java.util.Set;

import com.google.common.collect.ImmutableSet;

/** Encapsulates configuration values necessary to determine if and when to send notifications. */
public class WorkerConfig {
    private int burstDurationDays;
    private Set<String> burstStartEventIdSet = ImmutableSet.of();
    private String burstTaskId;
    private Set<String> excludedDataGroupSet = ImmutableSet.of();
    private int notificationBlackoutDaysFromStart;
    private int notificationBlackoutDaysFromEnd;
    private String notificationMessage;
    private int numMissedConsecutiveDaysToNotify;
    private int numMissedDaysToNotify;
    private Set<String> requiredSubpopulationGuidSet = ImmutableSet.of();

    /** The length of the study burst, in days. */
    public int getBurstDurationDays() {
        return burstDurationDays;
    }

    /** @see #getBurstDurationDays */
    public void setBurstDurationDays(int burstDurationDays) {
        this.burstDurationDays = burstDurationDays;
    }

    /** The set of activity event IDs that mark the start of a study burst. May be empty, but never null/ */
    public Set<String> getBurstStartEventIdSet() {
        return burstStartEventIdSet;
    }

    /** @see #getBurstStartEventIdSet */
    public void setBurstStartEventIdSet(Set<String> burstStartEventIdSet) {
        this.burstStartEventIdSet = burstStartEventIdSet != null ? burstStartEventIdSet : ImmutableSet.of();
    }

    /** Task ID that the participant is expected to complete or receive notifications. */
    public String getBurstTaskId() {
        return burstTaskId;
    }

    /** @see #getBurstTaskId */
    public void setBurstTaskId(String burstTaskId) {
        this.burstTaskId = burstTaskId;
    }

    /** Set of data groups that will never receive notifications. */
    public Set<String> getExcludedDataGroupSet() {
        return excludedDataGroupSet;
    }

    /** @see #getExcludedDataGroupSet */
    public void setExcludedDataGroupSet(Set<String> excludedDataGroupSet) {
        this.excludedDataGroupSet = excludedDataGroupSet != null ? excludedDataGroupSet : ImmutableSet.of();
    }

    /** Number of days at the start of the study burst where we don't send notifications. */
    public int getNotificationBlackoutDaysFromStart() {
        return notificationBlackoutDaysFromStart;
    }

    /** @see #getNotificationBlackoutDaysFromStart */
    public void setNotificationBlackoutDaysFromStart(int notificationBlackoutDaysFromStart) {
        this.notificationBlackoutDaysFromStart = notificationBlackoutDaysFromStart;
    }

    /** Number of days at the end of the study burst where we don't send notifications. */
    public int getNotificationBlackoutDaysFromEnd() {
        return notificationBlackoutDaysFromEnd;
    }

    /** @see #getNotificationBlackoutDaysFromEnd */
    public void setNotificationBlackoutDaysFromEnd(int notificationBlackoutDaysFromEnd) {
        this.notificationBlackoutDaysFromEnd = notificationBlackoutDaysFromEnd;
    }

    /** Notification message to send. */
    public String getNotificationMessage() {
        return notificationMessage;
    }

    /** @see #getNotificationMessage */
    public void setNotificationMessage(String notificationMessage) {
        this.notificationMessage = notificationMessage;
    }

    /** Number of consecutive days of missed activities before we send a notification. */
    public int getNumMissedConsecutiveDaysToNotify() {
        return numMissedConsecutiveDaysToNotify;
    }

    /** @see #getNumMissedConsecutiveDaysToNotify */
    public void setNumMissedConsecutiveDaysToNotify(int numMissedConsecutiveDaysToNotify) {
        this.numMissedConsecutiveDaysToNotify = numMissedConsecutiveDaysToNotify;
    }

    /** Number of cumulative days of missed activities within a single study burst before we send a notification. */
    public int getNumMissedDaysToNotify() {
        return numMissedDaysToNotify;
    }

    /** @see #getNumMissedDaysToNotify */
    public void setNumMissedDaysToNotify(int numMissedDaysToNotify) {
        this.numMissedDaysToNotify = numMissedDaysToNotify;
    }

    /** Set of subpopulations that the participant must be consented to in order to receive notifications. */
    public Set<String> getRequiredSubpopulationGuidSet() {
        return requiredSubpopulationGuidSet;
    }

    /** @see #getRequiredSubpopulationGuidSet */
    public void setRequiredSubpopulationGuidSet(Set<String> requiredSubpopulationGuidSet) {
        this.requiredSubpopulationGuidSet = requiredSubpopulationGuidSet != null ? requiredSubpopulationGuidSet :
                ImmutableSet.of();
    }
}

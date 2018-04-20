package org.sagebionetworks.bridge.notification.worker;

import java.util.Set;

public class WorkerConfig {
    private int burstDurationDays;
    private Set<String> burstStartEventIdSet;
    private String burstTaskId;
    private Set<String> excludedDataGroupSet;
    private int notificationBlackoutDaysFromStart;
    private int notificationBlackoutDaysFromEnd;
    private String notificationMessage;
    private int numMissedConsecutiveDaysToNotify;
    private int numMissedDaysToNotify;
    private Set<String> requiredSubpopulationGuidSet;

    public int getBurstDurationDays() {
        return burstDurationDays;
    }

    public void setBurstDurationDays(int burstDurationDays) {
        this.burstDurationDays = burstDurationDays;
    }

    public Set<String> getBurstStartEventIdSet() {
        return burstStartEventIdSet;
    }

    public void setBurstStartEventIdSet(Set<String> burstStartEventIdSet) {
        this.burstStartEventIdSet = burstStartEventIdSet;
    }

    public String getBurstTaskId() {
        return burstTaskId;
    }

    public void setBurstTaskId(String burstTaskId) {
        this.burstTaskId = burstTaskId;
    }

    public Set<String> getExcludedDataGroupSet() {
        return excludedDataGroupSet;
    }

    public void setExcludedDataGroupSet(Set<String> excludedDataGroupSet) {
        this.excludedDataGroupSet = excludedDataGroupSet;
    }

    public int getNotificationBlackoutDaysFromStart() {
        return notificationBlackoutDaysFromStart;
    }

    public void setNotificationBlackoutDaysFromStart(int notificationBlackoutDaysFromStart) {
        this.notificationBlackoutDaysFromStart = notificationBlackoutDaysFromStart;
    }

    public int getNotificationBlackoutDaysFromEnd() {
        return notificationBlackoutDaysFromEnd;
    }

    public void setNotificationBlackoutDaysFromEnd(int notificationBlackoutDaysFromEnd) {
        this.notificationBlackoutDaysFromEnd = notificationBlackoutDaysFromEnd;
    }

    public String getNotificationMessage() {
        return notificationMessage;
    }

    public void setNotificationMessage(String notificationMessage) {
        this.notificationMessage = notificationMessage;
    }

    public int getNumMissedConsecutiveDaysToNotify() {
        return numMissedConsecutiveDaysToNotify;
    }

    public void setNumMissedConsecutiveDaysToNotify(int numMissedConsecutiveDaysToNotify) {
        this.numMissedConsecutiveDaysToNotify = numMissedConsecutiveDaysToNotify;
    }

    public int getNumMissedDaysToNotify() {
        return numMissedDaysToNotify;
    }

    public void setNumMissedDaysToNotify(int numMissedDaysToNotify) {
        this.numMissedDaysToNotify = numMissedDaysToNotify;
    }

    public Set<String> getRequiredSubpopulationGuidSet() {
        return requiredSubpopulationGuidSet;
    }

    public void setRequiredSubpopulationGuidSet(Set<String> requiredSubpopulationGuidSet) {
        this.requiredSubpopulationGuidSet = requiredSubpopulationGuidSet;
    }
}

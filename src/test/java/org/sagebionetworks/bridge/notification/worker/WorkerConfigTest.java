package org.sagebionetworks.bridge.notification.worker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

public class WorkerConfigTest {
    private static final Set<String> DUMMY_SET = ImmutableSet.of("dummy string");

    @Test
    public void burstStartEventIdSetNeverNull() {
        // Initially empty
        WorkerConfig config = new WorkerConfig();
        assertTrue(config.getBurstStartEventIdSet().isEmpty());

        // Set works
        config.setBurstStartEventIdSet(DUMMY_SET);
        assertEquals(config.getBurstStartEventIdSet(), DUMMY_SET);

        // Set to null gives us an empty set
        config.setBurstStartEventIdSet(null);
        assertTrue(config.getBurstStartEventIdSet().isEmpty());
    }

    @Test
    public void excludedDataGroupSetNeverNull() {
        // Initially empty
        WorkerConfig config = new WorkerConfig();
        assertTrue(config.getExcludedDataGroupSet().isEmpty());

        // Set works
        config.setExcludedDataGroupSet(DUMMY_SET);
        assertEquals(config.getExcludedDataGroupSet(), DUMMY_SET);

        // Set to null gives us an empty set
        config.setExcludedDataGroupSet(null);
        assertTrue(config.getExcludedDataGroupSet().isEmpty());
    }

    @Test
    public void requiredSubpopulationGuidSetNeverNull() {
        // Initially empty
        WorkerConfig config = new WorkerConfig();
        assertTrue(config.getRequiredSubpopulationGuidSet().isEmpty());

        // Set works
        config.setRequiredSubpopulationGuidSet(DUMMY_SET);
        assertEquals(config.getRequiredSubpopulationGuidSet(), DUMMY_SET);

        // Set to null gives us an empty set
        config.setRequiredSubpopulationGuidSet(null);
        assertTrue(config.getRequiredSubpopulationGuidSet().isEmpty());
    }
}

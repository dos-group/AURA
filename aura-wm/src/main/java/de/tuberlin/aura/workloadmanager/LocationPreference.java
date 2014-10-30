package de.tuberlin.aura.workloadmanager;

import java.util.Collections;
import java.util.List;

import static de.tuberlin.aura.core.descriptors.Descriptors.MachineDescriptor;

public class LocationPreference {

    public enum PreferenceLevel {

        PREFERRED,

        REQUIRED
    }

    // ---------------------------------------------------
    // Fields.
    // ---------------------------------------------------

    public List<MachineDescriptor> preferredLocationAlternatives;

    public PreferenceLevel preferenceLevel;

    // ---------------------------------------------------
    // Constructors.
    // ---------------------------------------------------

    public LocationPreference(MachineDescriptor preferredLocation, PreferenceLevel preferenceLevel) {

        this(Collections.singletonList(preferredLocation), preferenceLevel);
    }

    public LocationPreference(List<MachineDescriptor> preferredLocationAlternatives, PreferenceLevel preferenceLevel) {

        this.preferredLocationAlternatives = preferredLocationAlternatives;

        this.preferenceLevel = preferenceLevel;
    }



}

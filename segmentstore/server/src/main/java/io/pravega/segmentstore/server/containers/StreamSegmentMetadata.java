/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.segmentstore.server.containers;

import com.google.common.base.Preconditions;
import io.pravega.common.Exceptions;
import io.pravega.common.util.ImmutableDate;
import io.pravega.segmentstore.server.ContainerMetadata;
import io.pravega.segmentstore.server.SegmentMetadata;
import io.pravega.segmentstore.server.UpdateableSegmentMetadata;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import lombok.extern.slf4j.Slf4j;

/**
 * Metadata for a particular Stream Segment.
 */
@Slf4j
@ThreadSafe
public class StreamSegmentMetadata implements UpdateableSegmentMetadata {
    //region Members

    private final String traceObjectId;
    private final String name;
    private final long streamSegmentId;
    private final int containerId;
    @GuardedBy("this")
    private final Map<UUID, Long> attributes;
    @GuardedBy("this")
    private long storageLength;
    @GuardedBy("this")
    private long startOffset;
    @GuardedBy("this")
    private long length;
    @GuardedBy("this")
    private boolean sealed;
    @GuardedBy("this")
    private boolean sealedInStorage;
    @GuardedBy("this")
    private boolean deleted;
    @GuardedBy("this")
    private boolean merged;
    @GuardedBy("this")
    private ImmutableDate lastModified;
    @GuardedBy("this")
    private long lastUsed;
    @GuardedBy("this")
    private boolean active;

    //endregion

    //region Constructor

    /**
     * Creates a new instance of the StreamSegmentMetadata class for a StreamSegment.
     *
     * @param streamSegmentName The name of the StreamSegment.
     * @param streamSegmentId   The Id of the StreamSegment.
     * @param containerId       The Id of the Container this StreamSegment belongs to.
     * @throws IllegalArgumentException If either of the arguments are invalid.
     */
    public StreamSegmentMetadata(String streamSegmentName, long streamSegmentId, int containerId) {
        Exceptions.checkNotNullOrEmpty(streamSegmentName, "streamSegmentName");
        Preconditions.checkArgument(streamSegmentId != ContainerMetadata.NO_STREAM_SEGMENT_ID, "streamSegmentId");
        Preconditions.checkArgument(containerId >= 0, "containerId");

        this.traceObjectId = String.format("StreamSegment[%d]", streamSegmentId);
        this.name = streamSegmentName;
        this.streamSegmentId = streamSegmentId;
        this.containerId = containerId;
        this.sealed = false;
        this.sealedInStorage = false;
        this.deleted = false;
        this.merged = false;
        this.startOffset = 0;
        this.storageLength = -1;
        this.length = -1;
        this.attributes = new HashMap<>();
        this.lastModified = new ImmutableDate();
        this.lastUsed = 0;
        this.active = true;
    }

    //endregion

    //region SegmentProperties Implementation

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public synchronized boolean isSealed() {
        return this.sealed;
    }

    @Override
    public synchronized boolean isDeleted() {
        return this.deleted;
    }

    @Override
    public synchronized ImmutableDate getLastModified() {
        return this.lastModified;
    }

    //endregion

    //region SegmentMetadata Implementation

    @Override
    public long getId() {
        return this.streamSegmentId;
    }

    @Override
    public int getContainerId() {
        return this.containerId;
    }

    @Override
    public synchronized boolean isMerged() {
        return this.merged;
    }

    @Override
    public synchronized boolean isSealedInStorage() {
        return this.sealedInStorage;
    }

    @Override
    public synchronized long getStorageLength() {
        return this.storageLength;
    }

    @Override
    public synchronized long getStartOffset() {
        return this.startOffset;
    }

    @Override
    public synchronized long getLength() {
        return this.length;
    }

    @Override
    public synchronized Map<UUID, Long> getAttributes() {
        return Collections.unmodifiableMap(this.attributes);
    }

    @Override
    public String toString() {
        return String.format(
                "Id = %d, Start = %d, Length = %d, StorageLength = %d, Sealed(M/S) = %s/%s, Deleted = %s, Name = %s",
                getId(),
                getStartOffset(),
                getLength(),
                getStorageLength(),
                isSealed(),
                isSealedInStorage(),
                isDeleted(),
                getName());
    }

    //endregion

    //region UpdateableSegmentMetadata Implementation

    @Override
    public synchronized void setStorageLength(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Storage Length must be a non-negative number.");
        Exceptions.checkArgument(value >= this.storageLength, "value", "New Storage Length cannot be smaller than the previous one.");

        log.trace("{}: StorageLength changed from {} to {}.", this.traceObjectId, this.storageLength, value);
        this.storageLength = value;
    }

    @Override
    public synchronized void setStartOffset(long value) {
        if (this.startOffset == value) {
            // Nothing to do.
            return;
        }

        Exceptions.checkArgument(value >= 0, "value", "StartOffset must be a non-negative number.");
        Exceptions.checkArgument(value >= this.startOffset, "value", "New StartOffset cannot be smaller than the previous one.");
        Exceptions.checkArgument(value <= this.length, "value", "New StartOffset cannot be larger than Length.");
        log.debug("{}: StartOffset changed from {} to {}.", this.traceObjectId, this.startOffset, value);
        this.startOffset = value;
    }

    @Override
    public synchronized void setLength(long value) {
        Exceptions.checkArgument(value >= 0, "value", "Length must be a non-negative number.");
        Exceptions.checkArgument(value >= this.length, "value", "New Length cannot be smaller than the previous one.");

        log.trace("{}: Length changed from {} to {}.", this.traceObjectId, this.length, value);
        this.length = value;
    }

    @Override
    public synchronized void markSealed() {
        log.debug("{}: Sealed = true.", this.traceObjectId);
        this.sealed = true;
    }

    @Override
    public synchronized void markSealedInStorage() {
        Preconditions.checkState(this.sealed, "Cannot mark SealedInStorage if not Sealed in DurableLog.");
        log.debug("{}: SealedInStorage = true.", this.traceObjectId);
        this.sealedInStorage = true;
    }

    @Override
    public synchronized void markDeleted() {
        log.debug("{}: Deleted = true.", this.traceObjectId);
        this.deleted = true;
    }

    @Override
    public synchronized void markMerged() {
        log.debug("{}: Merged = true.", this.traceObjectId);
        this.merged = true;
    }

    @Override
    public synchronized void setLastModified(ImmutableDate date) {
        this.lastModified = date;
        log.trace("{}: LastModified = {}.", this.lastModified);
    }

    @Override
    public synchronized void updateAttributes(Map<UUID, Long> attributes) {
        attributes.forEach(this.attributes::put);
    }

    @Override
    public synchronized void copyFrom(SegmentMetadata base) {
        Exceptions.checkArgument(this.getId() == base.getId(), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentId).");
        Exceptions.checkArgument(this.getName().equals(base.getName()), "base", "Given SegmentMetadata refers to a different StreamSegment than this one (SegmentName).");

        log.debug("{}: copyFrom {}.", this.traceObjectId, base.getClass().getSimpleName());
        setStorageLength(base.getStorageLength());
        setLength(base.getLength());

        // Update StartOffset after (potentially) updating the length, since he Start Offset must be less than or equal to Length.
        setStartOffset(base.getStartOffset());
        setLastModified(base.getLastModified());
        updateAttributes(base.getAttributes());

        if (base.isSealed()) {
            markSealed();
            if (base.isSealedInStorage()) {
                markSealedInStorage();
            }
        }

        if (base.isMerged()) {
            markMerged();
        }

        if (base.isDeleted()) {
            markDeleted();
        }

        setLastUsed(base.getLastUsed());
    }

    @Override
    public synchronized void setLastUsed(long value) {
        this.lastUsed = Math.max(value, this.lastUsed);
    }

    @Override
    public synchronized long getLastUsed() {
        return this.lastUsed;
    }

    @Override
    public synchronized boolean isActive() {
        return this.active;
    }

    synchronized void markInactive() {
        this.active = false;
    }

    //endregion
}

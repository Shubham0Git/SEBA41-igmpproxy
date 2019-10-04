/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.opencord.igmpproxy;

import java.util.concurrent.atomic.AtomicLong;

public class IgmpStatistics {

    //Total number of join requests
    private AtomicLong igmpJoinReq = new AtomicLong();
    //Total number of successful join and rejoin requests
    private AtomicLong igmpSuccessJoinRejoinReq = new AtomicLong();
    //Total number of failed join requests
    private AtomicLong igmpFailJoinReq = new AtomicLong();
    //Total number of leaves requests
    private AtomicLong igmpLeaveReq = new AtomicLong();
    // Total number of disconnects
    private AtomicLong igmpDisconnect = new AtomicLong();
    //Total number of GSSQ source specific query requests
    private AtomicLong igmpGssqReq = new AtomicLong();
    //Count of Total number of IGMPV3_MEMBERSHIP_QUERY
    private AtomicLong igmpv3MembershipQuery = new AtomicLong();
    //Count of IGMPV1_MEMBERSHIP_REPORT
    private AtomicLong igmpv1MemershipReport = new AtomicLong();
    //Count of IGMPV3_MEMBERSHIP_REPORT
    private AtomicLong igmpv3MembershipReport = new AtomicLong();
    //Count of IGMPV2_MEMBERSHIP_REPORT
    private AtomicLong igmpv2MembershipReport = new AtomicLong();
    //Count of TYPE_IGMPV2_LEAVE_GROUP
    private AtomicLong igmpv2LeaveGroup = new AtomicLong();
    //Total number of messages received.
    private AtomicLong totalMsgReceived = new AtomicLong();
    //Total number of IGMP messages received
    private AtomicLong igmpMsgReceived = new AtomicLong();
    //Total number of invalid IGMP messages received
    private AtomicLong invalidIgmpMsgReceived = new AtomicLong();
    //Total number of GMQ member query requests
    private AtomicLong igmpGmqReq = new AtomicLong();
    //Total number of GSQ specific query requests
    private AtomicLong igmpGsqReq = new AtomicLong();
    //Peak number of connections established
    private AtomicLong peakConnectionEstablished = new AtomicLong();
    //Peak number of disconnects performed per Second
    private AtomicLong peakDisconnectsPerformed = new AtomicLong();
    //Peak number of messages received
    private AtomicLong peakMsgReceivedPerSec = new AtomicLong();
    //Duration of peak messages received
    private AtomicLong durationOfPeakMsg = new AtomicLong();
    //Duration of peak number of disconnects
    private AtomicLong durationOfPeakDisconnects = new AtomicLong();
    //Peak number of connections established per second
    private AtomicLong peakconnectionEst = new AtomicLong();
    //Duration of the peak connections per second
    private AtomicLong durationOfPeakConnections = new AtomicLong();

    public Long getIgmpJoinReq() {
        return igmpJoinReq.get();
    }

    public Long getIgmpSuccessJoinRejoinReq() {
        return igmpSuccessJoinRejoinReq.get();
    }

    public Long getIgmpFailJoinReq() {
        return igmpFailJoinReq.get();
    }

    public Long getIgmpLeaveReq() {
        return igmpLeaveReq.get();
    }

    public Long getIgmpDisconnect() {
        return igmpDisconnect.get();
    }

    public Long getIgmpGssqReq() {
        return igmpGssqReq.get();
    }

    public Long getIgmpv3MembershipQuery() {
        return igmpv3MembershipQuery.get();
    }

    public Long getIgmpv1MemershipReport() {
        return igmpv1MemershipReport.get();
    }

    public Long getIgmpv3MembershipReport() {
        return igmpv3MembershipReport.get();
    }

    public Long getIgmpv2MembershipReport() {
        return igmpv2MembershipReport.get();
    }

    public Long getIgmpv2LeaveGroup() {
        return igmpv2LeaveGroup.get();
    }

    public Long getTotalMsgReceived() {
        return totalMsgReceived.get();
    }

    public Long getIgmpMsgReceived() {
        return igmpMsgReceived.get();
    }

    public Long getInvalidIgmpMsgReceived() {
        return invalidIgmpMsgReceived.get();
    }

    public Long getIgmpGmqReq() {
        return igmpGmqReq.get();
    }

    public Long getIgmpGsqReq() {
        return igmpGsqReq.get();
    }

    public Long getPeakMsgReceivedPerSec() {
        return peakMsgReceivedPerSec.get();
    }

    public void setPeakMsgReceivedPerSec(AtomicLong peakMessageReceived) {
        this.peakMsgReceivedPerSec = peakMessageReceived;
    }

    public Long getDurationOfPeakMsg() {
        return durationOfPeakMsg.get();
    }

    public Long getDurationOfPeakDisconnects() {
        return durationOfPeakDisconnects.get();
    }

    public void setDurationOfPeakDisconnects(AtomicLong durationOfPeakDisconnects) {
        this.durationOfPeakDisconnects = durationOfPeakDisconnects;
    }


    public void setDurationOfPeakMsg(AtomicLong durationOfPeakMsg) {
       this.durationOfPeakMsg = durationOfPeakMsg;
    }

    public Long getPeakDisconnectsPerformed() {
        return peakDisconnectsPerformed.get();
    }

    public void setPeakDisconnectsPerformed(AtomicLong peakDisconnectsPerformed) {
        this.peakDisconnectsPerformed = peakDisconnectsPerformed;
    }

    public Long getPeakConnectionEstablished() {
        return peakConnectionEstablished.get();
    }

    public void setPeakConnectionEstablished(AtomicLong peakConnectionEstablished) {
        this.peakConnectionEstablished = peakConnectionEstablished;
    }

    public Long getPeakconnectionEst() {
        return peakconnectionEst.get();
    }

    public void setPeakconnectionEst(AtomicLong peakconnectionEst) {
        this.peakconnectionEst = peakconnectionEst;
    }

    public Long getDurationOfPeakConnections() {
        return durationOfPeakConnections.get();
    }

    public void setDurationOfPeakConnections(AtomicLong durationOfPeakConnections) {
        this.durationOfPeakConnections = durationOfPeakConnections;
    }

    public void increaseIgmpJoinReq() {
        igmpJoinReq.incrementAndGet();
    }

    public void increaseIgmpSuccessJoinRejoinReq() {
        igmpSuccessJoinRejoinReq.incrementAndGet();
    }

    public void increaseIgmpFailJoinReq() {
        igmpFailJoinReq.incrementAndGet();
    }

    public void increaseIgmpLeaveReq() {
        igmpLeaveReq.incrementAndGet();
    }

    public void increaseIgmpDisconnect() {
        igmpDisconnect.incrementAndGet();
    }

    public void increaseIgmpGssqReq() {
        igmpGssqReq.incrementAndGet();
    }

    public void increaseIgmpv3MembershipQuery() {
        igmpv3MembershipQuery.incrementAndGet();
    }

    public void increaseIgmpv2MembershipReport() {
        igmpv2MembershipReport.incrementAndGet();
    }

    public void increaseIgmpv1MemershipReport() {
        igmpv1MemershipReport.incrementAndGet();
    }

    public void increaseIgmpv3MembershipReport() {
        igmpv3MembershipReport.incrementAndGet();
    }

    public void increaseIgmpv2LeaveGroup() {
        igmpv2LeaveGroup.incrementAndGet();
    }

    public void increaseInvalidIgmpMsgReceived() {
        invalidIgmpMsgReceived.incrementAndGet();
    }

    public void countIgmpMsgReceived() {
        long igmpMsgRcvd = igmpv3MembershipQuery.get();
        igmpMsgRcvd += igmpv1MemershipReport.get();
        igmpMsgRcvd += igmpv2MembershipReport.get();
        igmpMsgRcvd += igmpv3MembershipReport.get();
        igmpMsgRcvd += igmpv2LeaveGroup.get();
        igmpMsgRcvd += invalidIgmpMsgReceived.get();
        this.igmpMsgReceived = new AtomicLong(igmpMsgRcvd);
    }

    public void increaseTotalMsgReceived() {
        totalMsgReceived.incrementAndGet();
    }

    public void increaseIgmpGmqReq() {
        igmpGmqReq.incrementAndGet();
    }

    public void increaseIgmpGsqReq() {
        igmpGsqReq.incrementAndGet();
    }


}

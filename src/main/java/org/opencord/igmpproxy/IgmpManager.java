/*
 * Copyright 2017-present Open Networking Foundation
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.onlab.packet.EthType;
import org.onlab.packet.Ethernet;
import org.onlab.packet.IGMP;
import org.onlab.packet.IGMPGroup;
import org.onlab.packet.IGMPMembership;
import org.onlab.packet.IGMPQuery;
import org.onlab.packet.IPv4;
import org.onlab.packet.Ip4Address;
import org.onlab.packet.IpAddress;
import org.onlab.packet.VlanId;
import org.onosproject.core.ApplicationId;
import org.onosproject.core.CoreService;
import org.onosproject.mastership.MastershipService;
import org.onosproject.net.AnnotationKeys;
import org.onosproject.net.ConnectPoint;
import org.onosproject.net.DeviceId;
import org.onosproject.net.Port;
import org.onosproject.net.PortNumber;
import org.onosproject.net.config.ConfigFactory;
import org.onosproject.net.config.NetworkConfigEvent;
import org.onosproject.net.config.NetworkConfigListener;
import org.onosproject.net.config.NetworkConfigRegistry;
import org.onosproject.net.config.basics.McastConfig;
import org.onosproject.net.config.basics.SubjectFactories;
import org.onosproject.net.device.DeviceEvent;
import org.onosproject.net.device.DeviceListener;
import org.onosproject.net.device.DeviceService;
import org.onosproject.net.flow.DefaultTrafficTreatment;
import org.onosproject.net.flow.FlowRuleService;
import org.onosproject.net.flow.criteria.Criteria;
import org.onosproject.net.flowobjective.DefaultFilteringObjective;
import org.onosproject.net.flowobjective.FilteringObjective;
import org.onosproject.net.flowobjective.FlowObjectiveService;
import org.onosproject.net.flowobjective.Objective;
import org.onosproject.net.flowobjective.ObjectiveContext;
import org.onosproject.net.flowobjective.ObjectiveError;
import org.onosproject.mcast.api.McastRoute;
import org.onosproject.mcast.api.MulticastRouteService;
import org.onosproject.net.packet.InboundPacket;
import org.onosproject.net.packet.PacketContext;
import org.onosproject.net.packet.PacketProcessor;
import org.onosproject.net.packet.PacketService;
import org.opencord.cordconfig.access.AccessDeviceConfig;
import org.opencord.cordconfig.access.AccessDeviceData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Igmp process application, use proxy mode, support first join/ last leave , fast leave
 * period query and keep alive, packet out igmp message to uplink port features.
 */
@Component(immediate = true)
public class IgmpManager {

    private static final Class<AccessDeviceConfig> CONFIG_CLASS =
            AccessDeviceConfig.class;
    private static final Class<IgmpproxyConfig> IGMPPROXY_CONFIG_CLASS =
            IgmpproxyConfig.class;
    private static final Class<IgmpproxySsmTranslateConfig> IGMPPROXY_SSM_CONFIG_CLASS =
            IgmpproxySsmTranslateConfig.class;
    private static final Class<McastConfig> MCAST_CONFIG_CLASS =
            McastConfig.class;

    public static Map<String, GroupMember> groupMemberMap = Maps.newConcurrentMap();
    private static ApplicationId appId;
    private static Map<DeviceId, AccessDeviceData> oltData = new ConcurrentHashMap<>();
    private static int unSolicitedTimeout = 3; // unit is 1 sec
    private static int keepAliveCount = 3;
    private static int lastQueryInterval = 2;  //unit is 1 sec
    private static int lastQueryCount = 2;
    private static boolean fastLeave = true;
    private static boolean withRAUplink = true;
    private static boolean withRADownlink = false;
    private static boolean periodicQuery = true;
    private static short mvlan = 4000;
    private static byte igmpCos = 7;
    public static boolean connectPointMode = true;
    public static ConnectPoint connectPoint = null;
    private static ConnectPoint sourceDeviceAndPort = null;
    private static boolean enableIgmpProvisioning = false;

    private static final Integer MAX_PRIORITY = 10000;
    private static final String INSTALLED = "installed";
    private static final String REMOVED = "removed";
    private static final String INSTALLATION = "installation";
    private static final String REMOVAL = "removal";

    private static boolean pimSSmInterworking = false;
    private static final String DEFAULT_PIMSSM_HOST = "127.0.0.1";
    private final ScheduledExecutorService scheduledExecutorService =
            Executors.newScheduledThreadPool(1);
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected CoreService coreService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected PacketService packetService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MastershipService mastershipService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowRuleService flowRuleService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected DeviceService deviceService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected FlowObjectiveService flowObjectiveService;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected NetworkConfigRegistry networkConfig;
    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected MulticastRouteService multicastService;

    @Reference(cardinality = ReferenceCardinality.MANDATORY_UNARY)
    protected IgmpStatisticsService igmpStatisticsManager;

    private static final int DEFAULT_REPEAT_DELAY = 20;
    @Property(name = "statisticsGenerationEvent", intValue = DEFAULT_REPEAT_DELAY,
            label = "statisticsGenerationEvent")
    private int statisticsGenerationEvent = DEFAULT_REPEAT_DELAY;

    private IgmpPacketProcessor processor = new IgmpPacketProcessor();
    private Logger log = LoggerFactory.getLogger(getClass());
    private ApplicationId coreAppId;
    private Map<Ip4Address, Ip4Address> ssmTranslateTable = new ConcurrentHashMap<>();

    private InternalNetworkConfigListener configListener =
            new InternalNetworkConfigListener();
    private DeviceListener deviceListener = new InternalDeviceListener();
    private ConfigFactory<DeviceId, AccessDeviceConfig> configFactory = null;

    protected IgmpStatisticsEventPublisher igmpStatisticsPublisher;


    ScheduledFuture<?> scheduledFuture;
    protected CalculatePeak calculatePeak;
    ScheduledExecutorService executorForPeakValue;
    private static final int DEFAULT_PEAK_VALUE_REPEAT_DELAY = 1;
    @Property(name = "statisticsGenerationEventForPeakValue", intValue = DEFAULT_PEAK_VALUE_REPEAT_DELAY,
            label = "statisticsGenerationEventForPeakValue")
    private int statisticsGenerationEventForPeakValue = DEFAULT_PEAK_VALUE_REPEAT_DELAY;
    ScheduledExecutorService executorForIgmp;

    private long peakMsgCount;
    private long peakMsgDuration;
    private long peakDisconnectCount;
    private long peakDisconnectDuration;
    private long peakConnectionCount;
    private long peakConnectionDuration;
    List<Long> peakMsgCountList = new ArrayList<Long>();
    List<Long> peakDisconnectList = new ArrayList<Long>();
    List<Long> peakConnectionList = new ArrayList<Long>();

    private ConfigFactory<ApplicationId, IgmpproxyConfig> igmpproxyConfigFactory =
            new ConfigFactory<ApplicationId, IgmpproxyConfig>(
                    SubjectFactories.APP_SUBJECT_FACTORY, IGMPPROXY_CONFIG_CLASS, "igmpproxy") {
                @Override
                public IgmpproxyConfig createConfig() {
                    return new IgmpproxyConfig();
                }
            };
    private ConfigFactory<ApplicationId, IgmpproxySsmTranslateConfig> igmpproxySsmConfigFactory =
            new ConfigFactory<ApplicationId, IgmpproxySsmTranslateConfig>(
                    SubjectFactories.APP_SUBJECT_FACTORY, IGMPPROXY_SSM_CONFIG_CLASS, "ssmTranslate", true) {
                @Override
                public IgmpproxySsmTranslateConfig createConfig() {
                    return new IgmpproxySsmTranslateConfig();
                }
            };

    private int maxResp = 10; //unit is 1 sec
    private int keepAliveInterval = 120; //unit is 1 sec

    public static int getUnsolicitedTimeout() {
        return unSolicitedTimeout;
    }

    @Activate
    protected void activate() {
        log.info("Entering in activate");
        appId = coreService.registerApplication("org.opencord.igmpproxy");
        coreAppId = coreService.registerApplication(CoreService.CORE_APP_NAME);
        packetService.addProcessor(processor, PacketProcessor.director(4));
        IgmpSender.init(packetService, mastershipService);

        if (networkConfig.getConfigFactory(CONFIG_CLASS) == null) {
            configFactory =
                    new ConfigFactory<DeviceId, AccessDeviceConfig>(
                            SubjectFactories.DEVICE_SUBJECT_FACTORY, CONFIG_CLASS, "accessDevice") {
                        @Override
                        public AccessDeviceConfig createConfig() {
                            return new AccessDeviceConfig();
                        }
                    };
            networkConfig.registerConfigFactory(configFactory);
        }
        networkConfig.registerConfigFactory(igmpproxySsmConfigFactory);
        networkConfig.registerConfigFactory(igmpproxyConfigFactory);
        networkConfig.addListener(configListener);

        configListener.reconfigureNetwork(networkConfig.getConfig(appId, IGMPPROXY_CONFIG_CLASS));
        configListener.reconfigureSsmTable(networkConfig.getConfig(appId, IGMPPROXY_SSM_CONFIG_CLASS));

        networkConfig.getSubjects(DeviceId.class, AccessDeviceConfig.class).forEach(
                subject -> {
                    AccessDeviceConfig config = networkConfig.getConfig(subject,
                            AccessDeviceConfig.class);
                    if (config != null) {
                        AccessDeviceData data = config.getAccessDevice();
                        oltData.put(data.deviceId(), data);
                    }
                }
        );

        oltData.keySet().forEach(d -> provisionDefaultFlows(d));
        if (connectPointMode) {
            provisionConnectPointFlows();
        } else {
            provisionUplinkFlows();
        }

        McastConfig config = networkConfig.getConfig(coreAppId, MCAST_CONFIG_CLASS);
        if (config != null) {
            mvlan = config.egressVlan().toShort();
            IgmpSender.getInstance().setMvlan(mvlan);
        }
        deviceService.addListener(deviceListener);
        scheduledExecutorService.scheduleAtFixedRate(new IgmpProxyTimerTask(), 0, 1000, TimeUnit.MILLISECONDS);
        log.info("creating executor");
        executorForIgmp = Executors.newScheduledThreadPool(1);
        log.info("created executor");
        igmpStatisticsPublisher = new IgmpStatisticsEventPublisher();
        scheduledFuture = executorForIgmp.scheduleAtFixedRate(igmpStatisticsPublisher,
                0, statisticsGenerationEvent, TimeUnit.SECONDS);
        calculatePeak = new CalculatePeak();
        executorForPeakValue = Executors.newScheduledThreadPool(1);
        scheduledFuture = executorForPeakValue.scheduleAtFixedRate(calculatePeak, 0,
                                                  statisticsGenerationEventForPeakValue, TimeUnit.SECONDS);
        log.info("Started");
    }

    @Deactivate
    protected void deactivate() {
        scheduledExecutorService.shutdown();

        // de-register and null our handler
        networkConfig.removeListener(configListener);
        if (configFactory != null) {
            networkConfig.unregisterConfigFactory(configFactory);
        }
        networkConfig.unregisterConfigFactory(igmpproxyConfigFactory);
        networkConfig.unregisterConfigFactory(igmpproxySsmConfigFactory);
        deviceService.removeListener(deviceListener);
        packetService.removeProcessor(processor);
        flowRuleService.removeFlowRulesById(appId);
        scheduledFuture.cancel(true);
        executorForIgmp.shutdown();
        log.info("Stopped");
    }

    protected Ip4Address getDeviceIp(DeviceId ofDeviceId) {
        try {
            String[] mgmtAddress = deviceService.getDevice(ofDeviceId)
                    .annotations().value(AnnotationKeys.MANAGEMENT_ADDRESS).split(":");
            return Ip4Address.valueOf(mgmtAddress[0]);
        } catch (Exception ex) {
            log.info("No valid Ipaddress for " + ofDeviceId.toString());
            return null;
        }
    }

    private void processIgmpQuery(IGMPQuery igmpQuery, ConnectPoint cp, int maxResp) {

        DeviceId deviceId = cp.deviceId();
        Ip4Address gAddr = igmpQuery.getGaddr().getIp4Address();
        maxResp = calculateMaxResp(maxResp);
        if (gAddr != null && !gAddr.isZero()) {
            StateMachine.specialQuery(deviceId, gAddr, maxResp);
        } else {
            StateMachine.generalQuery(deviceId, maxResp);
        }
    }

    private void processIgmpConnectPointQuery(IGMPQuery igmpQuery, ConnectPoint cp, int maxResp) {

        DeviceId deviceId = cp.deviceId();
        Ip4Address gAddr = igmpQuery.getGaddr().getIp4Address();
        maxResp = calculateMaxResp(maxResp);
        //The query is received on the ConnectPoint
        // send query accordingly to the registered OLT devices.
        if (gAddr != null && !gAddr.isZero()) {
            for (DeviceId devId : oltData.keySet()) {
                StateMachine.specialQuery(devId, gAddr, maxResp);
            }
        } else {
            //Don't know which group is targeted by the query
            //So query all the members(in all the OLTs) and proxy their reports
            StateMachine.generalQuery(maxResp);
        }
    }


    private int calculateMaxResp(int maxResp) {
        if (maxResp >= 128) {
            int mant = maxResp & 0xf;
            int exp = (maxResp >> 4) & 0x7;
            maxResp = (mant | 0x10) << (exp + 3);
        }

        return (maxResp + 5) / 10;
    }

    private Ip4Address ssmTranslateRoute(IpAddress group) {
        return ssmTranslateTable.get(group);
    }

    private void processIgmpReport(IGMPMembership igmpGroup, VlanId vlan, ConnectPoint cp, byte igmpType) {
        DeviceId deviceId = cp.deviceId();
        PortNumber portNumber = cp.port();

        Ip4Address groupIp = igmpGroup.getGaddr().getIp4Address();
        if (!groupIp.isMulticast()) {
            log.info(groupIp.toString() + " is not a valid group address");
            return;
        }
        Ip4Address srcIp = getDeviceIp(deviceId);

        byte recordType = igmpGroup.getRecordType();
        boolean join = false;

        ArrayList<Ip4Address> sourceList = new ArrayList<>();

        if (igmpGroup.getSources().size() > 0) {
            igmpGroup.getSources().forEach(source -> sourceList.add(source.getIp4Address()));
            if (recordType == IGMPMembership.CHANGE_TO_EXCLUDE_MODE ||
                    recordType == IGMPMembership.MODE_IS_EXCLUDE ||
                    recordType == IGMPMembership.BLOCK_OLD_SOURCES) {
                join = false;
            } else if (recordType == IGMPMembership.CHANGE_TO_INCLUDE_MODE ||
                    recordType == IGMPMembership.MODE_IS_INCLUDE ||
                    recordType == IGMPMembership.ALLOW_NEW_SOURCES) {
                join = true;
            }
        } else {
            IpAddress src = null;
            if (pimSSmInterworking) {
                src = ssmTranslateRoute(groupIp);
                if (src == null) {
                    log.info("no ssm translate for group " + groupIp.toString());
                    return;
                }
            } else {
                src = IpAddress.valueOf(DEFAULT_PIMSSM_HOST);
            }
            sourceList.add(src.getIp4Address());
            if (recordType == IGMPMembership.CHANGE_TO_EXCLUDE_MODE ||
                    recordType == IGMPMembership.MODE_IS_EXCLUDE ||
                    recordType == IGMPMembership.BLOCK_OLD_SOURCES) {
                join = true;
            } else if (recordType == IGMPMembership.CHANGE_TO_INCLUDE_MODE ||
                    recordType == IGMPMembership.MODE_IS_INCLUDE ||
                    recordType == IGMPMembership.ALLOW_NEW_SOURCES) {
                join = false;
            }
        }
        String groupMemberKey = GroupMember.getkey(groupIp, deviceId, portNumber);
        GroupMember groupMember = groupMemberMap.get(groupMemberKey);

        if (join) {
            //TODO FOR Total number of join requests
            igmpStatisticsManager.getIgmpStats().increaseIgmpJoinReq();
            if (groupMember == null) {
                if (igmpType == IGMP.TYPE_IGMPV2_MEMBERSHIP_REPORT) {
                    groupMember = new GroupMember(groupIp, vlan, deviceId, portNumber, true);
                } else {
                    groupMember = new GroupMember(groupIp, vlan, deviceId, portNumber, false);
                }

                Optional<ConnectPoint> sourceConfigured = getSource();
                if (!sourceConfigured.isPresent()) {
                     //TODO for Total number of failed join requests
                     igmpStatisticsManager.getIgmpStats().increaseIgmpFailJoinReq();
                    log.warn("Unable to process IGMP Join from {} since no source " +
                                     "configuration is found.", deviceId);
                    return;
                }
                HashSet<ConnectPoint> sourceConnectPoints = Sets.newHashSet(sourceConfigured.get());

                boolean isJoined = StateMachine.join(deviceId, groupIp, srcIp);
                if (isJoined) {
                    //TODO for Total number of successful join and rejoin requests
                    igmpStatisticsManager.getIgmpStats().increaseIgmpSuccessJoinRejoinReq();
                    peakConnectionCount++;
                } else {
                    //TODO if isJoined is false then falid join should increase
                    igmpStatisticsManager.getIgmpStats().increaseIgmpFailJoinReq();
                }
                groupMemberMap.put(groupMemberKey, groupMember);
                groupMember.updateList(recordType, sourceList);
                groupMember.getSourceList().forEach(source -> {
                    McastRoute route = new McastRoute(source, groupIp, McastRoute.Type.IGMP);
                    //add route
                    multicastService.add(route);
                    //add source to the route
                    multicastService.addSources(route, Sets.newHashSet(sourceConnectPoints));
                    //add sink to the route
                    multicastService.addSinks(route, Sets.newHashSet(cp));
                });

            }
            groupMember.resetAllTimers();
            groupMember.updateList(recordType, sourceList);
            groupMember.setLeave(false);
        } else {
            // TODO Total number of leaves requests
            igmpStatisticsManager.getIgmpStats().increaseIgmpLeaveReq();
            if (groupMember == null) {
                log.info("receive leave but no instance, group " + groupIp.toString() +
                        " device:" + deviceId.toString() + " port:" + portNumber.toString());
                return;
            } else {
                groupMember.setLeave(true);

                if (fastLeave) {
                    leaveAction(groupMember);
                } else {
                    sendQuery(groupMember);
                }
            }
        }
    }

    private void leaveAction(GroupMember groupMember) {
        //TODO Total number of disconnects
        igmpStatisticsManager.getIgmpStats().increaseIgmpDisconnect();
        peakDisconnectCount++;
//        igmpStatisticsManager.calculatePeakFromList(false, true, false);
        ConnectPoint cp = new ConnectPoint(groupMember.getDeviceId(), groupMember.getPortNumber());
        StateMachine.leave(groupMember.getDeviceId(), groupMember.getGroupIp());
        groupMember.getSourceList().forEach(source -> multicastService.removeSinks(
                new McastRoute(source, groupMember.getGroupIp(),
                               McastRoute.Type.IGMP), Sets.newHashSet(cp)));
        groupMemberMap.remove(groupMember.getId());
    }

    private void sendQuery(GroupMember groupMember) {
        Ethernet ethpkt;
        Ip4Address srcIp = getDeviceIp(groupMember.getDeviceId());
        if (groupMember.getv2()) {
            ethpkt = IgmpSender.getInstance().buildIgmpV2Query(groupMember.getGroupIp(), srcIp);
        } else {
            ethpkt = IgmpSender.getInstance().buildIgmpV3Query(groupMember.getGroupIp(), srcIp);
            //TODO Total number of GSSQ source specific query requests
            igmpStatisticsManager.getIgmpStats().increaseIgmpGssqReq();
        }
        IgmpSender.getInstance().sendIgmpPacket(ethpkt, groupMember.getDeviceId(), groupMember.getPortNumber());
    }

    /**
     * @return connect point of the source if configured; and empty Optional otherwise.
     */
    public static Optional<ConnectPoint> getSource() {
        return sourceDeviceAndPort == null ? Optional.empty() :
                Optional.of(sourceDeviceAndPort);
    }

    /**
     * Packet processor responsible for forwarding packets along their paths.
     */
    private class IgmpPacketProcessor implements PacketProcessor {

        @Override
        public void process(PacketContext context) {

            try {
                peakMsgCount++;
                //TODO Total number of messages received.
                igmpStatisticsManager.getIgmpStats().increaseTotalMsgReceived();
                InboundPacket pkt = context.inPacket();
                Ethernet ethPkt = pkt.parsed();
                if (ethPkt == null) {
                //TODO
                    log.info("packet type is null");
                    return;
                }
                if (ethPkt.getEtherType() != Ethernet.TYPE_IPV4) {
                    //TODO
                    log.info("Ethernet type is not IPV4");
                    return;
                }

                IPv4 ipv4Pkt = (IPv4) ethPkt.getPayload();
                //TODO
                log.info("Protocol type is :" + ipv4Pkt.getProtocol());
                if (ipv4Pkt.getProtocol() != IPv4.PROTOCOL_IGMP) {
                //TODO
                log.info("protocol type is not ipv4");
                    return;
                }

                short vlan = ethPkt.getVlanID();
                DeviceId deviceId = pkt.receivedFrom().deviceId();
                if (oltData.get(deviceId) == null &&
                        !isConnectPoint(deviceId, pkt.receivedFrom().port())) {
                    log.error("Device not registered in netcfg :" + deviceId.toString());
                    return;
                }

                IGMP igmp = (IGMP) ipv4Pkt.getPayload();
                switch (igmp.getIgmpType()) {
                    case IGMP.TYPE_IGMPV3_MEMBERSHIP_QUERY:
                        //TODO
                        igmpStatisticsManager.getIgmpStats().increaseIgmpv3MembershipQuery();
                        //Discard Query from OLT’s non-uplink port’s
                        if (!pkt.receivedFrom().port().equals(getDeviceUplink(deviceId))) {
                            if (isConnectPoint(deviceId, pkt.receivedFrom().port())) {
                                log.info("IGMP Picked up query from connectPoint");
                                //OK to process packet
                                processIgmpConnectPointQuery((IGMPQuery) igmp.getGroups().get(0), pkt.receivedFrom(),
                                        0xff & igmp.getMaxRespField());
                                break;
                            } else {
                                //Not OK to process packet
                                log.warn("IGMP Picked up query from non-uplink port");
                                return;
                            }
                        }

                        processIgmpQuery((IGMPQuery) igmp.getGroups().get(0), pkt.receivedFrom(),
                                0xff & igmp.getMaxRespField());
                        break;
                    case IGMP.TYPE_IGMPV1_MEMBERSHIP_REPORT:
                        //TODO
                        igmpStatisticsManager.getIgmpStats().increaseIgmpv1MemershipReport();
                        log.debug("IGMP version 1  message types are not currently supported.");
                        break;
                    case IGMP.TYPE_IGMPV3_MEMBERSHIP_REPORT:
                        //TODO
                        igmpStatisticsManager.getIgmpStats().increaseIgmpv3MembershipReport();
                    case IGMP.TYPE_IGMPV2_MEMBERSHIP_REPORT:
                        //TODO
                         igmpStatisticsManager.getIgmpStats().increaseIgmpv2MembershipReport();
                    case IGMP.TYPE_IGMPV2_LEAVE_GROUP:
                        //TODO
                        igmpStatisticsManager.getIgmpStats().increaseIgmpv2LeaveGroup();
                        //Discard join/leave from OLT’s uplink port’s
                        if (pkt.receivedFrom().port().equals(getDeviceUplink(deviceId)) ||
                                isConnectPoint(deviceId, pkt.receivedFrom().port())) {
                            log.info("IGMP Picked up join/leave from uplink/connectPoint port");
                            return;
                        }

                        Iterator<IGMPGroup> itr = igmp.getGroups().iterator();
                        while (itr.hasNext()) {
                            IGMPGroup group = itr.next();
                            if (group instanceof IGMPMembership) {
                                processIgmpReport((IGMPMembership) group, VlanId.vlanId(vlan),
                                        pkt.receivedFrom(), igmp.getIgmpType());
                            } else if (group instanceof IGMPQuery) {
                                IGMPMembership mgroup;
                                mgroup = new IGMPMembership(group.getGaddr().getIp4Address());
                                mgroup.setRecordType(igmp.getIgmpType() == IGMP.TYPE_IGMPV2_MEMBERSHIP_REPORT ?
                                        IGMPMembership.MODE_IS_EXCLUDE : IGMPMembership.MODE_IS_INCLUDE);
                                processIgmpReport(mgroup, VlanId.vlanId(vlan),
                                        pkt.receivedFrom(), igmp.getIgmpType());
                            }
                        }
                        break;

                    default:
                        log.info("wrong IGMP v3 type:" + igmp.getIgmpType());
                        //TODO Total number of invalid IGMP messages received
                        igmpStatisticsManager.getIgmpStats().increaseInvalidIgmpMsgReceived();
                        break;
                }
                //TODO Total number of IGMP messages received
                igmpStatisticsManager.getIgmpStats().countIgmpMsgReceived();

            } catch (Exception ex) {
                log.error("igmp process error : {} ", ex);
                ex.printStackTrace();
            }
        }
    }

    private class IgmpProxyTimerTask extends TimerTask {
        public void run() {
            try {
                IgmpTimer.timeOut1s();
                queryMembers();
            } catch (Exception ex) {
                log.warn("Igmp timer task error : {}", ex.getMessage());
            }
        }

        private void queryMembers() {
            GroupMember groupMember;
            Set groupMemberSet = groupMemberMap.entrySet();
            Iterator itr = groupMemberSet.iterator();
            while (itr.hasNext()) {
                Map.Entry entry = (Map.Entry) itr.next();
                groupMember = (GroupMember) entry.getValue();
                DeviceId did = groupMember.getDeviceId();
                if (mastershipService.isLocalMaster(did)) {
                    if (groupMember.isLeave()) {
                        lastQuery(groupMember);
                    } else if (periodicQuery) {
                        periodicQuery(groupMember);
                    }
                }
            }
        }

        private void lastQuery(GroupMember groupMember) {
            if (groupMember.getLastQueryInterval() < lastQueryInterval) {
                groupMember.lastQueryInterval(true); // count times
            } else if (groupMember.getLastQueryCount() < lastQueryCount - 1) {
                sendQuery(groupMember);
                //TODO Total number of GSQ specific query requests
                igmpStatisticsManager.getIgmpStats().increaseIgmpGsqReq();
                groupMember.lastQueryInterval(false); // reset count number
                groupMember.lastQueryCount(true); //count times
            } else if (groupMember.getLastQueryCount() == lastQueryCount - 1) {
                leaveAction(groupMember);
            }
        }

        private void periodicQuery(GroupMember groupMember) {
            if (groupMember.getKeepAliveQueryInterval() < keepAliveInterval) {
                groupMember.keepAliveInterval(true);
            } else if (groupMember.getKeepAliveQueryCount() < keepAliveCount) {
                sendQuery(groupMember);
                //TODO Total number of GMQ member query requests
                igmpStatisticsManager.getIgmpStats().increaseIgmpGmqReq();
                groupMember.keepAliveInterval(false);
                groupMember.keepAliveQueryCount(true);
            } else if (groupMember.getKeepAliveQueryCount() == keepAliveCount) {
                leaveAction(groupMember);
            }
        }

    }

    public static PortNumber getDeviceUplink(DeviceId devId) {
        if (oltData.get(devId) != null) {
            return oltData.get(devId).uplink();
        } else {
            return null;
        }
    }

    private void processFilterObjective(DeviceId devId, PortNumber port, boolean remove) {
        if (!enableIgmpProvisioning) {
            log.debug("IGMP trap rules won't be installed since enableIgmpProvisioning flag is not set");
            return;
        }
        //TODO migrate to packet requests when packet service uses filtering objectives
        DefaultFilteringObjective.Builder builder = DefaultFilteringObjective.builder();

        builder = remove ? builder.deny() : builder.permit();

        FilteringObjective igmp = builder
                .withKey(Criteria.matchInPort(port))
                .addCondition(Criteria.matchEthType(EthType.EtherType.IPV4.ethType()))
                .addCondition(Criteria.matchIPProtocol(IPv4.PROTOCOL_IGMP))
                .withMeta(DefaultTrafficTreatment.builder().setOutput(PortNumber.CONTROLLER).build())
                .fromApp(appId)
                .withPriority(MAX_PRIORITY)
                .add(new ObjectiveContext() {
                    @Override
                    public void onSuccess(Objective objective) {
                        log.info("Igmp filter for {} on {} {}.",
                                 devId, port, (remove) ? REMOVED : INSTALLED);
                    }

                    @Override
                    public void onError(Objective objective, ObjectiveError error) {
                        log.info("Igmp filter {} for device {} on port {} failed because of {}",
                                 (remove) ? INSTALLATION : REMOVAL, devId, port,
                                 error);
                    }
                });

        flowObjectiveService.filter(devId, igmp);

    }

    private boolean isConnectPoint(DeviceId device, PortNumber port) {
        if (connectPoint != null) {
            return (connectPointMode && connectPoint.deviceId().equals(device)
                    && connectPoint.port().equals(port));
        } else {
            log.info("connectPoint not configured for device {}", device);
            return false;
        }
    }

    private boolean isUplink(DeviceId device, PortNumber port) {
        return ((!connectPointMode) && oltData.containsKey(device)
                && oltData.get(device).uplink().equals(port));
    }

    private class InternalDeviceListener implements DeviceListener {
        @Override
        public void event(DeviceEvent event) {
            DeviceId devId = event.subject().id();
            Port p = event.port();
            if (oltData.get(devId) == null &&
                    !(p != null && isConnectPoint(devId, p.number()))) {
                return;
            }
            PortNumber port;

            switch (event.type()) {

                case DEVICE_ADDED:
                case DEVICE_UPDATED:
                case DEVICE_REMOVED:
                case DEVICE_SUSPENDED:
                case DEVICE_AVAILABILITY_CHANGED:
                case PORT_STATS_UPDATED:
                    break;
                case PORT_ADDED:
                    port = p.number();
                    if (oltData.containsKey(devId) && !isUplink(devId, port) && !isConnectPoint(devId, port)) {
                        processFilterObjective(devId, port, false);
                    } else if (isUplink(devId, port)) {
                        provisionUplinkFlows();
                    } else if (isConnectPoint(devId, port)) {
                        provisionConnectPointFlows();
                    }
                    break;
                case PORT_UPDATED:
                    port = p.number();
                    if (oltData.containsKey(devId) && !isUplink(devId, port) && !isConnectPoint(devId, port)) {
                        if (event.port().isEnabled()) {
                            processFilterObjective(devId, port, false);
                        } else {
                            processFilterObjective(devId, port, true);
                        }
                    } else if (isUplink(devId, port)) {
                        if (event.port().isEnabled()) {
                            provisionUplinkFlows(devId);
                        } else {
                            processFilterObjective(devId, port, true);
                        }
                    } else if (isConnectPoint(devId, port)) {
                        if (event.port().isEnabled()) {
                            provisionConnectPointFlows();
                        } else {
                            unprovisionConnectPointFlows();
                        }
                    }
                    break;
                case PORT_REMOVED:
                    port = p.number();
                    processFilterObjective(devId, port, true);
                    break;
                default:
                    log.info("Unknown device event {}", event.type());
                    break;
            }
        }

        @Override
        public boolean isRelevant(DeviceEvent event) {
            return true;
        }
    }

    private class InternalNetworkConfigListener implements NetworkConfigListener {

        private void reconfigureNetwork(IgmpproxyConfig cfg) {
            IgmpproxyConfig newCfg = cfg == null ? new IgmpproxyConfig() : cfg;

            unSolicitedTimeout = newCfg.unsolicitedTimeOut();
            maxResp = newCfg.maxResp();
            keepAliveInterval = newCfg.keepAliveInterval();
            keepAliveCount = newCfg.keepAliveCount();
            lastQueryInterval = newCfg.lastQueryInterval();
            lastQueryCount = newCfg.lastQueryCount();
            withRAUplink = newCfg.withRAUplink();
            withRADownlink = newCfg.withRADownlink();
            igmpCos = newCfg.igmpCos();
            periodicQuery = newCfg.periodicQuery();
            fastLeave = newCfg.fastLeave();
            pimSSmInterworking = newCfg.pimSsmInterworking();
            enableIgmpProvisioning = newCfg.enableIgmpProvisioning();

            if (connectPointMode != newCfg.connectPointMode() ||
                    connectPoint != newCfg.connectPoint()) {
                connectPointMode = newCfg.connectPointMode();
                connectPoint = newCfg.connectPoint();
                if (connectPointMode) {
                    unprovisionUplinkFlows();
                    provisionConnectPointFlows();
                } else {
                    unprovisionConnectPointFlows();
                    provisionUplinkFlows();
                }
            }
            if (connectPoint != null) {
                log.info("connect point : {}", connectPoint);
            }
            log.info("mode: {}", connectPointMode);

            getSourceConnectPoint(newCfg);

            IgmpSender.getInstance().setIgmpCos(igmpCos);
            IgmpSender.getInstance().setMaxResp(maxResp);
            IgmpSender.getInstance().setMvlan(mvlan);
            IgmpSender.getInstance().setWithRADownlink(withRADownlink);
            IgmpSender.getInstance().setWithRAUplink(withRAUplink);
        }

        void getSourceConnectPoint(IgmpproxyConfig cfg) {
            sourceDeviceAndPort = cfg.getSourceDeviceAndPort();
            if (sourceDeviceAndPort != null) {
                log.debug("source parameter configured to {}", sourceDeviceAndPort);
            }
        }

        public void reconfigureSsmTable(IgmpproxySsmTranslateConfig cfg) {
            if (cfg == null) {
                return;
            }
            Collection<McastRoute> translations = cfg.getSsmTranslations();
            for (McastRoute route : translations) {
                ssmTranslateTable.put(route.group().getIp4Address(), route.source().get().getIp4Address());
            }
        }

        @Override
        public void event(NetworkConfigEvent event) {
            switch (event.type()) {
                case CONFIG_ADDED:
                case CONFIG_UPDATED:
                    if (event.configClass().equals(CONFIG_CLASS)) {
                        AccessDeviceConfig config =
                                networkConfig.getConfig((DeviceId) event.subject(), CONFIG_CLASS);
                        if (config != null) {
                            oltData.put(config.getAccessDevice().deviceId(), config.getAccessDevice());
                            provisionDefaultFlows((DeviceId) event.subject());
                            provisionUplinkFlows((DeviceId) event.subject());
                        }
                    }

                    if (event.configClass().equals(IGMPPROXY_CONFIG_CLASS)) {
                        IgmpproxyConfig config = networkConfig.getConfig(appId, IGMPPROXY_CONFIG_CLASS);
                        if (config != null) {
                            log.info("igmpproxy config received. {}", config);
                            reconfigureNetwork(config);
                        }
                    }

                    if (event.configClass().equals(IGMPPROXY_SSM_CONFIG_CLASS)) {
                        IgmpproxySsmTranslateConfig config = networkConfig.getConfig(appId, IGMPPROXY_SSM_CONFIG_CLASS);
                        if (config != null) {
                            reconfigureSsmTable(config);
                        }
                    }

                    if (event.configClass().equals(MCAST_CONFIG_CLASS)) {
                        McastConfig config = networkConfig.getConfig(coreAppId, MCAST_CONFIG_CLASS);
                        if (config != null && mvlan != config.egressVlan().toShort()) {
                            mvlan = config.egressVlan().toShort();
                            IgmpSender.getInstance().setMvlan(mvlan);
                            groupMemberMap.values().forEach(m -> leaveAction(m));
                        }
                    }

                    log.info("Reconfigured");
                    break;
                case CONFIG_REGISTERED:
                case CONFIG_UNREGISTERED:
                    break;
                case CONFIG_REMOVED:
                    if (event.configClass().equals(CONFIG_CLASS)) {
                        oltData.remove(event.subject());
                    }

                default:
                    break;
            }
        }
    }

    private void provisionDefaultFlows(DeviceId deviceId) {
        List<Port> ports = deviceService.getPorts(deviceId);
        ports.stream()
                .filter(p -> (!oltData.get(p.element().id()).uplink().equals(p.number()) && p.isEnabled()))
                .forEach(p -> processFilterObjective((DeviceId) p.element().id(), p.number(), false));
    }

    private void provisionUplinkFlows(DeviceId deviceId) {
        if (connectPointMode) {
            return;
        }

        processFilterObjective(deviceId, oltData.get(deviceId).uplink(), false);
    }

    private void provisionUplinkFlows() {
        if (connectPointMode) {
            return;
        }

        oltData.keySet().forEach(deviceId -> provisionUplinkFlows(deviceId));
    }
    private void unprovisionUplinkFlows() {
        oltData.keySet().forEach(deviceId ->
                processFilterObjective(deviceId, oltData.get(deviceId).uplink(), true));
    }

    private void provisionConnectPointFlows() {
        if ((!connectPointMode) || connectPoint == null) {
            return;
        }

        processFilterObjective(connectPoint.deviceId(), connectPoint.port(), false);
    }
    private void unprovisionConnectPointFlows() {
        if (connectPoint == null) {
            return;
        }
        processFilterObjective(connectPoint.deviceId(), connectPoint.port(), true);
    }

    private class IgmpStatisticsEventPublisher implements Runnable {
        public void run() {
            log.info("Notifying IgmpStatisticsEvent");
            log.debug("IgmpJoinReq  ======= " + igmpStatisticsManager.getIgmpStats().getIgmpJoinReq());
            log.debug("IgmpSuccessJoinRejoinReq ======= " + igmpStatisticsManager.
                    getIgmpStats().getIgmpSuccessJoinRejoinReq());
            log.debug("IgmpFailJoinReq ======= " + igmpStatisticsManager.getIgmpStats().getIgmpFailJoinReq());
            log.debug("IgmpLeaveReq ======= " + igmpStatisticsManager.getIgmpStats().getIgmpLeaveReq());
            log.debug("IgmpDisconnect ======= " + igmpStatisticsManager.getIgmpStats().getIgmpDisconnect());
            log.debug("IgmpGssqReq ======= " + igmpStatisticsManager.getIgmpStats().getIgmpGssqReq());
            log.debug("TotalMsgReceived ======= " + igmpStatisticsManager.getIgmpStats().getTotalMsgReceived());
            log.debug("IgmpMsgReceived ======= " + igmpStatisticsManager.getIgmpStats().getIgmpMsgReceived());
            log.debug("InvalidIgmpMsgReceived =======" + igmpStatisticsManager.getIgmpStats().
                                                          getInvalidIgmpMsgReceived());
            log.debug("IgmpGmqReq ======= " + igmpStatisticsManager.getIgmpStats().getIgmpGmqReq());
            log.debug("IgmpGsqReq ======= " + igmpStatisticsManager.getIgmpStats().getIgmpGsqReq());
            log.debug("IgmpPeakMsgReceivedPerSec =========" + igmpStatisticsManager.
                                                          getIgmpStats().getPeakMsgReceivedPerSec());
            log.debug("IgmpDurationOfPeakMsg ======= " + igmpStatisticsManager.
                                                          getIgmpStats().getDurationOfPeakMsg());
            log.debug("IgmpPeakDisconnectsPerformed =======" + igmpStatisticsManager.
                                                          getIgmpStats().getPeakDisconnectsPerformed());
            log.debug("IgmpDurationOfPeakDisconnects =======" + igmpStatisticsManager.
                                                          getIgmpStats().getDurationOfPeakDisconnects());
            log.debug("IgmpPeakConnectionEst ====== " + igmpStatisticsManager.getIgmpStats().
                                                          getPeakconnectionEst());
            log.debug("IgmpDurationOfPeakConnection ======= " + igmpStatisticsManager.getIgmpStats().
                                                          getDurationOfPeakConnections());
            igmpStatisticsManager.getStatsDelegate().
                 notify(new IgmpStatisticsEvent(IgmpStatisticsEvent.Type.STATS_UPDATE,
                                                                    igmpStatisticsManager.getIgmpStats()));
        }
    }

    private class CalculatePeak implements Runnable {
        public void run() {
            log.debug("Calculating peaks");

            //Peak message per second and duration of peak message
            peakMsgCountList.add(peakMsgCount);
            if (peakMsgCount > peakMsgCountList.get(peakMsgCountList.size() - 1)) {
                //We got new peak value for message received
                peakMsgDuration = 0;
            } else {
                peakMsgDuration++;
            }
            igmpStatisticsManager.getIgmpStats().setPeakMsgReceivedPerSec(new AtomicLong(peakMsgCount));
            igmpStatisticsManager.getIgmpStats().setDurationOfPeakMsg(new AtomicLong(peakMsgDuration));
            peakMsgCount = 0;

            //Peak Disconnects per Second and duration of Peak Disconnects
            peakDisconnectList.add(peakDisconnectCount);
            if (peakDisconnectCount > peakDisconnectList.get(peakDisconnectList.size() - 1)) {
                //We got new Peak Value for Disconnect
                peakDisconnectDuration = 0;
           } else {
                peakDisconnectDuration++;
           }
            igmpStatisticsManager.getIgmpStats().setPeakDisconnectsPerformed(new AtomicLong(peakDisconnectCount));
            igmpStatisticsManager.getIgmpStats().setDurationOfPeakDisconnects(new AtomicLong(peakDisconnectDuration));
            peakDisconnectCount = 0;

            //Peak Connect per Second and duration of Peak connects
            peakConnectionList.add(peakConnectionCount);
            if (peakConnectionCount > peakConnectionList.get(peakConnectionList.size() - 1)) {
                //We get new Peak Value
                peakConnectionDuration = 0;
            } else {
                peakConnectionDuration++;
            }
            igmpStatisticsManager.getIgmpStats().setPeakconnectionEst(new AtomicLong(peakConnectionCount));
            igmpStatisticsManager.getIgmpStats().setDurationOfPeakConnections(new AtomicLong(peakConnectionDuration));
            peakConnectionCount = 0;

        }
    }
}
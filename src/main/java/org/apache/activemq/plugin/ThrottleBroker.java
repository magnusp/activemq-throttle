package org.apache.activemq.plugin;

import com.espertech.esper.client.*;
import org.apache.activemq.advisory.AdvisoryBroker;
import org.apache.activemq.broker.*;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.security.SecurityContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * User: Magnus Persson
 * Date: Mar 24, 2010
 * Time: 10:48:12 AM
 */
public class ThrottleBroker extends AdvisoryBroker implements UpdateListener {
    private static final Log LOG = LogFactory.getLog(ThrottleBroker.class);
    private final EPServiceProvider epService;
    private final static ActiveMQTopic advisoryDestination = new ActiveMQTopic("ThrottlingBrokerPlugin");
    private final Map<String, List<String>> usernameConnectionIdMapping = new HashMap<String, List<String>>();

    public ThrottleBroker(Broker next) {
        super(next);
        Configuration config = new Configuration();
        config.addEventTypeAutoName("org.apache.activemq.plugin");
        Map typeMap = new HashMap();
        typeMap.put("size", double.class);
        typeMap.put("connectionId", String.class);
        typeMap.put("username", String.class);
        config.addEventType("ThrottleBrokerEvent", typeMap);

        epService = EPServiceProviderManager.getDefaultProvider(config);
    }

    public EPStatement addExpression(String expression) {
        return epService.getEPAdministrator().createEPL(expression);
    }

    public void addListeningExpression(String expression) {
        addExpression(expression).addListener(this);
    }

    public void addConnection(ConnectionContext connectionContext, ConnectionInfo connectionInfo) throws Exception {
        SecurityContext securityContext = connectionContext.getSecurityContext();
        if (securityContext != null) {
            String username = connectionContext.getSecurityContext().getUserName();


            List<String> cids = usernameConnectionIdMapping.get(username);
            if (cids == null) {
                cids = new ArrayList<String>();
                usernameConnectionIdMapping.put(username, cids);
            }
            cids.add(connectionInfo.getConnectionId().toString());
            LOG.debug("Add connection for: " + username);
        }

        next.addConnection(connectionContext, connectionInfo);
    }

    public void removeConnection(ConnectionContext connectionContext, ConnectionInfo connectionInfo, Throwable throwable) throws Exception {
        SecurityContext securityContext = connectionContext.getSecurityContext();
        if (securityContext != null) {
            String username = connectionContext.getSecurityContext().getUserName();
            List<String> cids = usernameConnectionIdMapping.get(username);
            if(cids!=null) {
                for (String cid : cids) {
                    if (cid.equals(connectionInfo.getConnectionId().toString())) {
                        cids.remove(cid);
                        break;
                    }
                }
                LOG.debug("Remove connection for: " + username);
            }
        }

        next.removeConnection(connectionContext, connectionInfo, throwable);
    }

    public void send(ProducerBrokerExchange producerExchange, Message messageSend) throws Exception {
        EPRuntime epr = epService.getEPRuntime();
        Map eventMap = new HashMap();
        eventMap.put("size", messageSend.getSize());
        eventMap.put("connectionId", producerExchange.getConnectionContext().getConnectionId().toString());
        epr.sendEvent(eventMap, "ThrottleBrokerEvent");
        next.send(producerExchange, messageSend);
    }

    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        try {
            if (newEvents != null) {
                for (EventBean eb : newEvents) {
                    if (eb.get("rate") == null)
                        break;

                    String username = null;
                    for (Map.Entry<String, List<String>> mapping : usernameConnectionIdMapping.entrySet()) {
                        for (String cid : mapping.getValue()) {
                            if (cid.equals(eb.get("connectionId"))) {
                                username = mapping.getKey();
                                break;
                            }
                        }
                        if (username != null)
                            break;
                    }

                    if (username != null && usernameConnectionIdMapping != null) {
                        for (String cid : usernameConnectionIdMapping.get(username)) {
                            for (TransportConnector tp : getBrokerService().getTransportConnectors()) {
                                for (TransportConnection tpc : tp.getConnections()) {
                                    if (tpc.getConnectionId().equals(cid)) {
                                        adviseThrottled(username);
                                        tpc.serviceException(new BrokerStoppedException("Throttled"));
                                        LOG.info("Throttled");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void adviseThrottled(String username) throws Exception {
        LOG.debug("Sending advisory message");
        ActiveMQTextMessage advisoryMessage = new ActiveMQTextMessage();
        advisoryMessage.setText("User throttled: " + username);
        ConnectionContext context = new ConnectionContext();
        context.setSecurityContext(SecurityContext.BROKER_SECURITY_CONTEXT);
        context.setBroker(getBrokerService().getBroker());
        fireAdvisory(context, advisoryDestination, null, null, advisoryMessage);
    }
}

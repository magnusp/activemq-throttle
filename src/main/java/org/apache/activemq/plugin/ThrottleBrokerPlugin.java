package org.apache.activemq.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.InitializingBean;

/**
 * User: Magnus Persson
 * Date: Mar 24, 2010
 * Time: 10:48:25 AM
 */
public class ThrottleBrokerPlugin implements BrokerPlugin, InitializingBean {
    private static Log LOG = LogFactory.getLog(ThrottleBrokerPlugin.class);
    private int rateThrottle = 10;
    private int sizeThrottle = 4096;

    public ThrottleBrokerPlugin() {
    }

    public ThrottleBrokerPlugin(int rateThrottle, int sizeThrottle) {
        this.rateThrottle = rateThrottle;
        this.sizeThrottle = sizeThrottle;
    }

    public void setRateThrottle(int rateThrottle) {
        this.rateThrottle = rateThrottle;
    }

    public void setSizeThrottle(int sizeThrottle) {
        this.sizeThrottle = sizeThrottle;
    }

    public Broker installPlugin(Broker broker) throws Exception {
        LOG.info("Installing ThrottleBroker plugin");

        ThrottleBroker teb = new ThrottleBroker(broker);
        teb.addExpression("create window RateWindow.win:length(1) (connectionId string, rate double)");
        teb.addExpression("insert into RateWindow select rate(1) as rate, connectionId from ThrottleBrokerEvent group by connectionId output snapshot every 1 events");
        teb.addListeningExpression("select * from RateWindow where rate>" + rateThrottle + " group by connectionId output snapshot every 3 events");
        return teb;
    }

    public void afterPropertiesSet() throws Exception {
        LOG.info("Created ThrottleBroker plugin: " + this.toString());
    }
}

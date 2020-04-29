package fof.daq.hub;


import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.bridge.BridgeEventType;
import io.vertx.ext.bridge.BridgeOptions;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.eventbus.bridge.tcp.TcpEventBusBridge;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;



/*
@Component
public class TcpServerVerticle extends AbstractVerticle {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    static String tcpServerAddress = "FOF.DAQ.CW.SERVER.";

    static String tcpClientAddress = "FOF.DAQ.CW.CLIENT.";

    @Autowired
    private EventBus eb;

    @Autowired
    private TcpCaptchaHandler tcpCaptchaHandler;

    @Autowired
    private TcpVerificationHandler tcpVerificationHandler;

    @Autowired
    private TcpRegisterHandler tcpRegisterHandler;

    @Autowired
    @Qualifier("SERVER_ADDRESS")
    private String serverAddress;

    @Autowired
    @Qualifier("CLIENT_ADDRESS")
    private String clientAddress;

    @Autowired
    @Qualifier("config")
    private JsonObject config;

    @Override
    public void start() throws Exception {
        super.start();
        TcpEventBusBridge bridge = TcpEventBusBridge.create(vertx,
                new BridgeOptions()
                .addInboundPermitted(new PermittedOptions().setAddressRegex("FOF.*"))
                .addOutboundPermitted(new PermittedOptions().setAddressRegex( "FOF.*")),
                null,
                it -> {
                    if(it.type() != BridgeEventType.SOCKET_PING) {
                        logger.warn(">>>> BridgeEventType: " + it.type());
                        System.out.println(it.getRawMessage());
                    }
                    it.complete(true);
                });
        // 注册服务
        eb.localConsumer(tcpServerAddress + "REGISTER", tcpRegisterHandler);
        // 获取验证码图片
        eb.localConsumer(tcpServerAddress + "CAPTCHA", tcpCaptchaHandler);
        // 二次验证服务触发
        eb.localConsumer(tcpServerAddress + "VERIFICATION", tcpVerificationHandler);

        int tcpPort = config.getInteger("TCP.PORT", 8080);
        bridge.listen(tcpPort, res -> {
            if (res.succeeded()) {
                logger.info("Success start TCP port:" + tcpPort);
            } else {
                logger.error(res.cause());
            }
        });

    }
}
*/

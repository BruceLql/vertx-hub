/*
package fof.daq.hub.handler;

import fof.daq.hub.utils.NoSuchServerException;
import fof.daq.hub.utils.RetryWithTimeOut;
import fof.daq.hub.utils.StringUtils;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.SharedData;
import io.vertx.rxjava.core.buffer.Buffer;
import io.vertx.rxjava.ext.web.client.HttpResponse;
import io.vertx.rxjava.ext.web.client.WebClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import rx.Observable;
import java.util.concurrent.TimeUnit;

*/
/**
 * PY机器分配
 * *//*

public class ElectCrawlerHandler implements Handler<Message<JsonObject>> {
    private Logger logger = LoggerFactory.getLogger(this.getClass());
    public static String Address = "INIT";

    @Autowired
    private SharedData sd;

    @Autowired
    private WebClient client;

    @Autowired
    private EventBus eb;

    */
/**
     *  message.body() 参数
     * mobile
     * password 服务密码
     * isp 运营商
     * code 验证码
     * *//*

    @Override
    public void handle(Message<JsonObject> message){


        JsonObject body = message.body();
        if (body == null) {
            message.fail(0, "无请求参数");
            return;
        }
        String mobile = StringUtils.getJsonString(body, "mobile");  // 手机号码
        if (mobile == null || mobile.equals("")) {
            message.fail(0, "手机号码不能为空");
            return;
        }

        String mid = this.getMobileMid(mobile);  // 读取缓存
        if(mid != null) {
            logger.info("[Already exists cache MID[" + mid + "]");
            message.<JsonObject>reply(new JsonObject().put("mid", mid), it -> this.verificationHandler(it, mid));
            return;
        }
        JsonObject json  = new JsonObject();
        json.put("data", new JsonObject().put("mobile", "150002087712").put("ip", "10.168.1.127:8080").put("uid", "222"));
        // 选择连接服务
        chooseCrawler(mobile, json)
        .timeout(20, TimeUnit.SECONDS) // 超时时间限制在20秒
        .subscribe(
                ar -> this.filterMessage(message, ar, mobile),
                err -> {
                    // 返回错误内容
                    message.fail(0, err.getMessage());
                    logger.error(err);
                });
    }

    */
/**
     * 过滤消息
     * *//*

    private void filterMessage(Message<JsonObject> message, HttpResponse<Buffer> ar, String mobile) {
        if (ar.statusCode() != 200) {
            logger.warn("Server status code:" + ar.statusCode());
            logger.warn("Server error info:" + ar.body());
            message.fail(1, "已连接服务器，请求内容失败");
            return;
        }
        try {
            JsonObject body = ar.bodyAsJsonObject();
            String mid = StringUtils.getJsonString(body, "mid");
            if (mid == null) {
                message.fail(1, "服务器返回数据错误");
                logger.error(new NullPointerException("Mid is null"));
            } else {
                // 发送验证地址请求
                logger.info("Request mid:" + mid);
                this.setMobileMid(mobile, mid);
                message.<JsonObject>reply(new JsonObject().put("mid", mid), it -> this.verificationHandler(it, mid));
            }
        } catch (Exception e) {
            message.fail(1, "服务器数据解析错误");
            logger.error(e);
        }
    }

    */
/**
     * 验证注册地址是否注册成功
     * *//*

    private void verificationHandler(AsyncResult<Message<JsonObject>> ar, String mid) {
        if (ar.succeeded()) {
            String address = StringUtils.getJsonString(ar.result().body(), "address");
            if (address == null) {
                logger.error(new NullPointerException("Verification address cannot be null:" + ar.result().toString()));
            } else {
                logger.info("Verification address[" + address + "] to MID[" + mid + "]");
                eb.send(address, new JsonObject().put("mid", mid));
            }
        } else {
            logger.error(ar.cause());
        }
    }

    */
/**
     * 将号码保存， 如果是集群需更换为集群模式
     * *//*

    private void setMobileMid(String mobile, String mid) {
        this.sd.getLocalMap("CRAWLER-SERVER").put(mobile, mid);
    }

    */
/**
     * 根据手机号码获取MID
     * *//*

    private String getMobileMid(String mobile) {
        return this.sd.<String, String>getLocalMap("CRAWLER-SERVER").get(mobile);
    }

    */
/**
     * 连接采集程序群
     * *//*

    private Observable<HttpResponse<Buffer>> chooseCrawler(String mobile, JsonObject json) {
       return Observable.defer(() -> this.crawlerServer(mobile).flatMap(url -> {
           return client.postAbs(url)
                   .rxSendJson(json)
                   .doOnSubscribe(() -> logger.info("Try connect url:" + url))
                   .doOnError(err -> logger.error("Connect error url:" + url))
                   .toObservable()
                   .timeout(5000, TimeUnit.MILLISECONDS) // 单个URL超时时间为5秒
                   .doOnError(err -> logger.error("Timeout error url:" + url))
                   .retryWhen(new RetryWithTimeOut(1)); // 发射连接超时就尝试一次重连当前地址
       })).retryWhen(obs -> obs.flatMap(throwable -> {
           if (throwable instanceof NoSuchServerException) {
               return Observable.<HttpResponse>error(throwable);
           }
           logger.warn("---- Try next server ----");
           return Observable.just(null);
       }));
    }

    */
/**
     * 模拟可用地址连接池
     * *//*

    private Observable<String> crawlerServer(String mobile) {
        String[] arrays = {null, "http://10.168.1.127:5000/cmcc", "http://192.168.1.11:5000", null, null, null};
        final int rnd = (int)(Math.random() * 4);
        if (arrays[rnd] == null) { // 模拟找不到合适的服务器时
           // return Observable.error(new NoSuchServerException("找不到可用服务器"));
        }
        return Observable.just("http://10.168.1.197:5000/cucc");//arrays[rnd]
    }
}
*/

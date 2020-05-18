import fof.daq.hub.common.utils.MD5;
import org.junit.Test;
import vts.jwt.JWTAuth;
import vts.jwt.JWTOptions;
import vts.jwt.json.JsonObject;

public class BuildTestToken {
    @Test
    public void token() {
        JWTAuth jwt = JWTAuth.create(new JsonObject()
                .put("keyStore", new JsonObject()
                        .put("type", "jceks")               // 签名文件类型
                        .put("path", "/Users/changcaichao/work/project/HUB/src/main/resources/keystore.jceks")    // 签名测试文件
                        .put("password", "secret")));       // 签名文件密码
        // 设置生成token的参数
        JWTOptions options = new JWTOptions();
        // 增加需要传递的参数
        JsonObject context = new JsonObject()
                .put("mobile", "18735812924")
                .put("isp", "CMCC")
                .put("userId", "11111111111")
                .put("callBack", "http://www.baidu.com")
                .put("name", "测试name")
                .put("cid", "22222222222222")
                .put("notifyUrl", "https://www.hao123.com")
                .put("nonce", String.valueOf(System.currentTimeMillis()));
        // 生成token
        String token = jwt.generateToken(context, options);
        System.out.println(MD5.INSTANCE.digest(token));
        System.out.println(token);
        try{
            // 验证token
            System.out.println(jwt.authenticate(token));
         //   User user = jwt.authenticate(token);
            System.out.println(">> TOKEN RESULT:");
            //获取token的JSON内容参数
        //    System.out.println(user.principal());

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}

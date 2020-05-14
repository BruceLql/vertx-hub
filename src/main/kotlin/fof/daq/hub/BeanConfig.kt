package fof.daq.hub
import io.vertx.core.json.JsonObject
import io.vertx.ext.auth.KeyStoreOptions
import io.vertx.ext.auth.jwt.JWTAuthOptions
import io.vertx.rxjava.core.Vertx
import io.vertx.rxjava.ext.asyncsql.AsyncSQLClient
import io.vertx.rxjava.ext.asyncsql.MySQLClient
import io.vertx.rxjava.ext.auth.jwt.JWTAuth
import io.vertx.rxjava.ext.web.Router
import io.vertx.rxjava.ext.web.client.WebClient
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.context.annotation.Bean
import tech.kavi.vs.core.rxjava.VertxBeans

/**
 * 依赖参数全局初始化
 * */
open class BeanConfig : VertxBeans() {

    /**
     * 注入router
     */
    @Bean
    fun router(vertx: Vertx): Router = Router.router(vertx)

    /**
     * 权限校验证书
     * */
    @Bean
    fun jwt(vertx: Vertx): JWTAuth = JWTAuth.create(vertx,
            JWTAuthOptions().setKeyStore(
                    KeyStoreOptions().setPath("keystore.jceks")
                            .setType("jceks")
                            .setPassword("secret")
            )
    )

    /**
     * api请求服务
     * */
    @Bean
    fun client(vertx: Vertx): WebClient = WebClient.create(vertx)


    /**
     * mysql数据库连接
     * */
    @Bean
    fun mysqlClient(rxVertx: io.vertx.rxjava.core.Vertx, @Qualifier("config") config: JsonObject): AsyncSQLClient{
        return MySQLClient.createShared(rxVertx, config.value("MYSQL", JsonObject()))
    }
}

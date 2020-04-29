package fof.daq.hub

import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.annotation.ComponentScan
import org.springframework.context.annotation.Import
import tech.kavi.vs.core.RxLauncherVerticle

/**
 * 主程序入口
 * */
@Import(BeanConfig::class)
@ComponentScan
class MainVerticle : RxLauncherVerticle() {

    @Autowired
    private lateinit var webVerticle: WebVerticle


    @Throws(Exception::class)
    override fun start() {
        super.start()
        vertx.deployVerticle(webVerticle)
    }

    companion object {
        @JvmStatic
        fun main(args:Array<String>) {
            // 初始化类
            launcher(MainVerticle::class.java)
        }
    }
}

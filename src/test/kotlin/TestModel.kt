import fof.daq.hub.model.Customer
import org.junit.Test
import kotlin.reflect.full.allSuperclasses
import kotlin.reflect.full.declaredMemberProperties
import kotlin.reflect.full.memberProperties
import kotlin.reflect.jvm.javaField

class TestModel{
    @Test
    fun testModel() {
        println(Customer::class.declaredMemberProperties.mapNotNull { it.name })
    }
}
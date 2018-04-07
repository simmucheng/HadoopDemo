import com.example.tutorial.AddressBookProtos;
import com.example.tutorial.JavaSerializableBean;
import org.junit.Test;

import java.io.*;

public class ProtobufTest {
    /**
     * 将protobuf序列化后的文件写入磁盘
     * @throws Exception
     */
    @Test
    public void write() throws Exception {

        AddressBookProtos.Person tom=AddressBookProtos.Person.newBuilder()
                .setId(12345)
                .setName("tom")
                .setEmail("123@123.123")
                .addPhone(AddressBookProtos.Person.PhoneNumber.newBuilder()
                            .setNumber("+351 999 999 999")
                            .setType(AddressBookProtos.Person.PhoneType.HOME)
                            .build())
                .build();
        tom.writeTo(new FileOutputStream("/Users/simmucheng/tmp/protobuf_test/persion.data"));
    }

    /**
     * 将写入磁盘的序列化文件反序列化读取出来
     * @throws Exception
     */
    @Test
    public void read() throws Exception {

        AddressBookProtos.Person tom=AddressBookProtos.Person.parseFrom(
                new FileInputStream("/Users/simmucheng/tmp/protobuf_test/persion.data"));
        System.out.println(tom.getName());
    }
    @Test
    public void SerializableWrite() throws Exception {
        JavaSerializableBean bean=new JavaSerializableBean();
        bean.setEmail("123@123.123");
        bean.setName("tom");
        bean.setId(12345);
        bean.setPhoneNo("+351 999 999 999");
        ObjectOutputStream obj=new ObjectOutputStream(
                new FileOutputStream("/Users/simmucheng/tmp/protobuf_test/bean.data"));
        obj.writeObject(bean);
        obj.close();
    }
    @Test
    public void SerializableRead() throws Exception {
        ObjectInputStream obj=new ObjectInputStream(
                new FileInputStream("/Users/simmucheng/tmp/protobuf_test/bean.data"));
        JavaSerializableBean bean= (JavaSerializableBean) obj.readObject();
        System.out.println(bean.getName());
    }

}

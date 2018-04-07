import Tutorialspoint.Employee;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class AvroTest {
    /**
     * Test avro write to disk
     */
    @Test
    public void TestWrite() throws Exception {
        //创建对象
        Employee e1=new Employee();
        e1.setName("tom");
        e1.setAge(20);
        //创建write对象
        DatumWriter<Employee> writer=new SpecificDatumWriter<Employee>(Employee.class);
        //创建写入file的writer对象
        DataFileWriter<Employee> dataFileWriter=new DataFileWriter<Employee>(writer);
        //串行化到磁盘
        dataFileWriter.create(e1.getSchema(), new File("/Users/simmucheng/tmp/avro_test/data.avro"));
        dataFileWriter.append(e1);
        dataFileWriter.append(e1);
        dataFileWriter.append(e1);
        dataFileWriter.append(e1);
        //关闭流
        dataFileWriter.close();
    }
    @Test
    public void TestRead() throws Exception {
        DatumReader<Employee> reader=new SpecificDatumReader<Employee>(Employee.class);
        DataFileReader<Employee> dataFileReader=new DataFileReader<Employee>(
                new File("/Users/simmucheng/tmp/avro_test/data.avro"),reader);
        while(dataFileReader.hasNext()){
            System.out.println(dataFileReader.next().getName());
        }
        dataFileReader.close();

    }

    /**
     * 不进行编译使用avro串行化
     */
    @Test
    public void TestWriter2() throws IOException {
        //初始化schema
        Schema schema = new Schema.Parser().parse(new File("/Users/simmucheng/tmp/avro_test/emp.avsc"));
        //e1相当于Employee
        GenericRecord e1=new GenericData.Record(schema);
        e1.put("Name","tom");
        e1.put("age",20);
        DatumWriter datumWriter = new SpecificDatumWriter(schema);
        DataFileWriter dataFileWriter=new DataFileWriter(datumWriter);
        dataFileWriter.create(schema,new File("/Users/simmucheng/tmp/avro_test/data1.avro"));
        dataFileWriter.append(e1);
        dataFileWriter.append(e1);
        dataFileWriter.append(e1);
        dataFileWriter.close();
    }
    @Test
    public void TestReader2() throws IOException {
        //初始化schema
        Schema schema = new Schema.Parser().parse(new File("/Users/simmucheng/tmp/avro_test/emp.avsc"));

        DatumReader<GenericRecord> datumReader = new SpecificDatumReader<GenericRecord>(schema);
        DataFileReader dataFileReader=new DataFileReader(new File("/Users/simmucheng/tmp/avro_test/data1.avro"),datumReader);
        GenericRecord tmp=null;
        while(dataFileReader.hasNext()){
            tmp= (GenericRecord) dataFileReader.next();
            System.out.println(tmp.get("Name"));
        }

    }
}

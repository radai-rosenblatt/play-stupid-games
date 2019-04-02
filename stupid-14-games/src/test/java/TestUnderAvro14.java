import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import net.radai.simple.SimpleEnumField;
import net.radai.union.UnionEnumField;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Test;


public class TestUnderAvro14 {

  @Test
  public void verifyAvro14() throws Exception {
    Map<String, URL> avroJarsByVersion = new HashMap<>();

    Enumeration<URL> manifests = Thread.currentThread().getContextClassLoader().getResources(JarFile.MANIFEST_NAME);
    while (manifests.hasMoreElements()) {
      URL url = manifests.nextElement();
      if (!url.toString().contains("avro")) {
        continue;
      }
      InputStream is = url.openStream();
      if (is != null) {
        Manifest manifest = new Manifest(is);
        Attributes avroAttrs = manifest.getAttributes("org/apache/avro");
        if (avroAttrs != null) {
          //this is how 1.4 packs stuff ¯\_(ツ)_/¯
          String version = avroAttrs.getValue("Implementation-Version");
          if (version == null) {
            throw new IllegalStateException("found an avro jar with no version in the manifest: " + url);
          }
          avroJarsByVersion.put(version, url);
        }
        Attributes mainAttrs = manifest.getMainAttributes();
        if ("Apache Avro".equalsIgnoreCase(mainAttrs.getValue("Specification-Title"))) {
          //this is how 1.8 packs stuff ¯\_(ツ)_/¯
          String version = mainAttrs.getValue("Specification-Version");
          if (version == null) {
            throw new IllegalStateException("found an avro jar with no version in the manifest: " + url);
          }
          avroJarsByVersion.put(version, url);
        }
      }
    }

    Assert.assertEquals("expected a single avro jar, got " + avroJarsByVersion.values(), 1, avroJarsByVersion.size());
    String version = avroJarsByVersion.keySet().iterator().next();
    URL jar = avroJarsByVersion.get(version);
    Assert.assertEquals("expecting avro 1.4.1, got " + version + "@" + jar, "1.4.1", version);
  }

  @Test
  public void showProperSimpleFieldWorks() throws Exception {
    Schema simpleEnumSchema = SimpleEnumField.SCHEMA$;
    GenericData.Record properGenericSimple = new GenericData.Record(simpleEnumSchema);
    properGenericSimple.put("f", new GenericData.EnumSymbol("B"));
    serialize(properGenericSimple);
  }

  @Test
  public void showMalformedSimpleFieldSadlyWorks() throws Exception {
    Schema simpleEnumSchema = SimpleEnumField.SCHEMA$;
    GenericData.Record badGenericSimple = new GenericData.Record(simpleEnumSchema);
    badGenericSimple.put("f", net.radai.simple.Enum.B); //mixing specific Enum and a generic record :-(
    serialize(badGenericSimple);
  }

  @Test
  public void showProperUnionFieldWorks() throws Exception {
    Schema unionEnumSchema = UnionEnumField.SCHEMA$;
    GenericData.Record properGenericUnion = new GenericData.Record(unionEnumSchema);
    properGenericUnion.put("f", new GenericData.EnumSymbol("B"));
    serialize(properGenericUnion);
  }

  @Test(expected = AvroRuntimeException.class)
  public void showMalformedUnionFieldExplodes() throws Exception {
    Schema unionEnumSchema = UnionEnumField.SCHEMA$;
    GenericData.Record badGenericUnion = new GenericData.Record(unionEnumSchema);
    badGenericUnion.put("f", net.radai.union.Enum.B);
    serialize(badGenericUnion);
  }

  private static byte[] serialize(IndexedRecord record) throws IOException {
    Schema schema = record.getSchema();
    try (ByteArrayOutputStream os = new ByteArrayOutputStream()) {
      BinaryEncoder encoder = new BinaryEncoder(os);
      DatumWriter<Object> writer;
      if (record instanceof SpecificRecord) {
        writer = new SpecificDatumWriter<>(schema);
      } else {
        writer = new GenericDatumWriter<>(schema);
      }
      writer.write(record, encoder);
      encoder.flush();
      return os.toByteArray();
    }
  }
}

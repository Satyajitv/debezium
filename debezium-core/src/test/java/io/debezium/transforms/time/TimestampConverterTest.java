package io.debezium.transforms.time;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by svegesna on 12/26/17.
 */
public class TimestampConverterTest {

    private static final TimeZone UTC = TimeZone.getTimeZone("UTC");
    private static final Calendar EPOCH;
    private static final Calendar TIME;
    private static final Calendar DATE;
    private static final Calendar DATE_PLUS_TIME;
    private static final long DATE_PLUS_TIME_UNIX;
    private static final String STRING_DATE_FMT = "yyyy MM dd HH mm ss SSS z";
    private static final String DATE_PLUS_TIME_STRING;
    private static final Schema payloadSchema;
    private static final Struct payloadValue;

    private final io.debezium.transforms.time.TimestampConverter<SourceRecord> xformKey = new io.debezium.transforms.time.TimestampConverter.Key<>();
    private final io.debezium.transforms.time.TimestampConverter<SourceRecord> xformValue = new io.debezium.transforms.time.TimestampConverter.Value<>();

    static {
        EPOCH = GregorianCalendar.getInstance(UTC);
        EPOCH.setTimeInMillis(0L);

        TIME = GregorianCalendar.getInstance(UTC);
        TIME.setTimeInMillis(0L);
        TIME.add(Calendar.MILLISECOND, 1234);

        DATE = GregorianCalendar.getInstance(UTC);
        DATE.setTimeInMillis(0L);
        DATE.set(1970, Calendar.JANUARY, 1, 0, 0, 0);
        DATE.add(Calendar.DATE, 1);

        DATE_PLUS_TIME = GregorianCalendar.getInstance(UTC);
        DATE_PLUS_TIME.setTimeInMillis(0L);
        DATE_PLUS_TIME.add(Calendar.DATE, 1);
        DATE_PLUS_TIME.add(Calendar.MILLISECOND, 1234);

        DATE_PLUS_TIME_UNIX = DATE_PLUS_TIME.getTime().getTime();
        DATE_PLUS_TIME_STRING = "1970 01 02 00 00 01 234 UTC";

        //AFTER Struct field
        SchemaBuilder after = SchemaBuilder.struct();
        after.field("id", Schema.INT32_SCHEMA);
        after.field("category", Schema.STRING_SCHEMA);
        after.field("created_at", io.debezium.time.Timestamp.builder());
        after.field("updated_at", Schema.INT64_SCHEMA);
        after.field("deleted_at", io.debezium.time.Timestamp.builder());
        Schema afterSchema = after.name("serverName.databaseName.tableName.Value").build();

        Struct afterValue = new Struct(afterSchema);
        afterValue.put("id", 101);
        afterValue.put("category", "sample");
        afterValue.put("created_at", 1510237013999L);
        afterValue.put("updated_at", 1510237013999L);
        afterValue.put("deleted_at", 1510237019999L);

        //AFTER Struct field

        //source Struct field
        SchemaBuilder source = SchemaBuilder.struct();
        source.field("name", Schema.STRING_SCHEMA);
        source.field("server_id", Schema.STRING_SCHEMA);
        Schema sourceSchema = source.build();

        Struct sourceValue = new Struct(sourceSchema);
        sourceValue.put("name", "xyz");
        sourceValue.put("server_id","12345");
        //source Struct field

        //before Struct field
        SchemaBuilder before = SchemaBuilder.struct();
        before.field("id", Schema.INT32_SCHEMA);
        before.field("category", Schema.STRING_SCHEMA);
        Schema beforeSchema = before.build();

        Struct beforeValue = new Struct(beforeSchema);
        beforeValue.put("id", 102);
        beforeValue.put("category","sample");
        //before Struct field

        //payload Struct field
        SchemaBuilder payload = SchemaBuilder.struct().name("serverName.databaseName.tableName.Envelope").optional().version(1)
                .field("after",afterSchema)
                .field("before", beforeSchema)
                .field("source", sourceSchema)
                .field("op", Schema.STRING_SCHEMA)
                .field("ts_ms", Schema.INT64_SCHEMA);
        payloadSchema = payload.build();
        payloadValue = new Struct(payloadSchema);
        payloadValue.put("after",afterValue);
        payloadValue.put("before",beforeValue);
        payloadValue.put("source",sourceValue);
        payloadValue.put("op","c");
        payloadValue.put("ts_ms",1510237013611L);
    }


    // Configuration

    @After
    public void teardown() {
        xformKey.close();
        xformValue.close();
    }

    @Test(expected = ArrayIndexOutOfBoundsException.class)
    public void testConfigNoTargetType() {
        xformValue.configure(Collections.<String, String>emptyMap());
    }

    @Test(expected = ConfigException.class)
    public void testConfigInvalidFieldType() {
        xformValue.configure(Collections.singletonMap(io.debezium.transforms.time.TimestampConverter.FIELD_CONFIG, "invalid"));
    }

    @Test(expected = ConfigException.class)
    public void testConfigMissingFormat() {
        xformValue.configure(Collections.singletonMap(io.debezium.transforms.time.TimestampConverter.TIMESTAMP_FORMAT_CONFIG, "bad-format"));
    }

    //TimestampConverter option1: which would accept fields and timestamp format(Converts all specified fields into their destination timestamp types)
    //"transforms.TimestampConv.fields":"<databasename>.<tablename>.<after/before>.<nestedfield>-><destinationtype>
    // || <databasename>.<tablename>.<field>-><destinationtype>"
    //"transforms.TimestampConv.timestamp.format":"yyyy-MM-dd hh:mm:ss"
    @Test(expected = ConfigException.class)
    public void testConfigTCoptionOneFormat() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_CONFIG, "invalid");
        config.put(TimestampConverter.TIMESTAMP_FORMAT_CONFIG, "bad-format");
        xformValue.configure(config);
    }

    //TimestampConverter option2: which would accept field.type and timestamp format(Converts all specified field.types present into their destination timestamp/other types)
    //field.type":"io.debezium.time.Timestamp->string,io.debezium.time.Date->string
    @Test(expected = ConfigException.class)
    public void testConfigTCoptionTwoFormat() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_TYPE_CONFIG, "invalid");
        config.put(TimestampConverter.TIMESTAMP_FORMAT_CONFIG, "bad-format");
        xformValue.configure(config);
    }

    //Combination of both TC option1 and option2. (Converts fields into destination format specified in FIELD_CONFIG + Converts fields to destination types based on the FIELD_TYPE_CONFIG)
    //Note: FIELD_TYPE_CONFIG would not be able to convert field types , if the fields are specified in FIELD_CONFIG.
    @Test(expected = ConfigException.class)
    public void testConfigTCBothOptionFormat() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_TYPE_CONFIG, "invalid");
        config.put(TimestampConverter.FIELD_CONFIG, "invalid");
        config.put(TimestampConverter.TIMESTAMP_FORMAT_CONFIG, "bad-format");
        xformValue.configure(config);
    }

    // Schema less conversion
    //Not going to be a valid case for Debezium, as Debezium messages would always have schema.
    @Test
    public void testSchemalessConversion() {
        xformValue.configure(Collections.singletonMap(TimestampConverter.FIELD_CONFIG, "database.table.after.field->Timestamp"));
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, null, DATE_PLUS_TIME.getTime()));

        assertNull(transformed.valueSchema());
        assertEquals(DATE_PLUS_TIME.getTime(), transformed.value());
    }

    // Test with Schema, conversions.
    //TimestampConverter option1: which would accept fields and timestamp format(Converts all specified fields into their destination timestamp types)
    @Test
    public void testWithSchemaOptionOneSingleField() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_CONFIG, "databaseName.tableName.after.created_at->string");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payloadSchema , payloadValue));

        Struct debeziumStruct = (Struct) transformed.value();
        assertEquals("2017-11-09 14:16:53",(((Struct) debeziumStruct.get("after")).get("created_at")).toString());
        assertEquals(1510237013999L,(((Struct) debeziumStruct.get("after")).get("updated_at")));
    }

    //TimestampConverter option1: which would accept fields and timestamp format(Converts all specified fields into their destination timestamp types)
    @Test
    public void testWithSchemaOptionOne() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_CONFIG, "databaseName.tableName.after.created_at->string,databaseName.tableName.after.updated_at->string");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payloadSchema , payloadValue));

        Struct debeziumStruct = (Struct) transformed.value();
        assertEquals("2017-11-09 14:16:53",(((Struct) debeziumStruct.get("after")).get("created_at")).toString());
        assertEquals("2017-11-09 14:16:53",(((Struct) debeziumStruct.get("after")).get("updated_at")).toString());
    }

    //TimestampConverter option2: which would accept field.type and timestamp format(Converts all specified field.types present into their destination timestamp/other types)
    //field.type":"io.debezium.time.Timestamp->string,io.debezium.time.Date->string
    @Test
    public void testWithSchemaOptionTwo() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_TYPE_CONFIG, "io.debezium.time.Timestamp->string,io.debezium.time.Date->string");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payloadSchema , payloadValue));

        Struct debeziumStruct = (Struct) transformed.value();
        assertEquals("2017-11-09 14:16:53",(((Struct) debeziumStruct.get("after")).get("created_at")).toString());
        assertEquals(1510237013999L,(((Struct) debeziumStruct.get("after")).get("updated_at")));
    }

    //Combination of both TC option1 and option2. (Converts fields into destination format specified in FIELD_CONFIG + Converts fields to destination types based on the FIELD_TYPE_CONFIG)
    //Note: FIELD_TYPE_CONFIG would not be able to convert field types , if the fields are specified in FIELD_CONFIG.
    @Test
    public void testWithSchemaBothOption() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_TYPE_CONFIG, "io.debezium.time.Timestamp->Date");
        config.put(TimestampConverter.FIELD_CONFIG, "databaseName.tableName.after.deleted_at->string");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payloadSchema , payloadValue));

        Struct debeziumStruct = (Struct) transformed.value();
        assertEquals("Wed Nov 08 16:00:00 PST 2017",(((Struct) debeziumStruct.get("after")).get("created_at")).toString());
        assertEquals(1510237013999L,(((Struct) debeziumStruct.get("after")).get("updated_at")));
        assertEquals("2017-11-09 14:16:59",(((Struct) debeziumStruct.get("after")).get("deleted_at")).toString());
    }

    @Test
    public void testWithSchemaBothOption2() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_TYPE_CONFIG, "io.debezium.time.Timestamp->Date");
        config.put(TimestampConverter.FIELD_CONFIG, "databaseName.tableName.after.deleted_at->string,databaseName.tableName.after.created_at->string");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payloadSchema , payloadValue));

        Struct debeziumStruct = (Struct) transformed.value();
        assertEquals("2017-11-09 14:16:53",(((Struct) debeziumStruct.get("after")).get("created_at")).toString());
        assertEquals(1510237013999L,(((Struct) debeziumStruct.get("after")).get("updated_at")));
        assertEquals("2017-11-09 14:16:59",(((Struct) debeziumStruct.get("after")).get("deleted_at")).toString());
    }

    //Testing non Struct fields, which should not call schemaForNestedStruct,valueForNestedStruct
    @Test
    public void testWithSchemaNonStructField() {
        Map<String, String> config = new HashMap<>();
        config.put(TimestampConverter.FIELD_TYPE_CONFIG, "io.debezium.time.Timestamp->Date");
        config.put(TimestampConverter.FIELD_CONFIG, "databaseName.tableName.ts_ms->string,databaseName.tableName.after.created_at->string");
        xformValue.configure(config);
        SourceRecord transformed = xformValue.apply(new SourceRecord(null, null, "topic", 0, payloadSchema , payloadValue));

        Struct debeziumStruct = (Struct) transformed.value();
        assertEquals("2017-11-09 14:16:53",(((Struct) debeziumStruct.get("after")).get("created_at")).toString());
        assertEquals(1510237013999L,(((Struct) debeziumStruct.get("after")).get("updated_at")));
        assertEquals("2017-11-09 14:16:53", debeziumStruct.get("ts_ms").toString());
    }

    //Test databaseTableString
    @Test
    public void databaseTableStringMethod(){
        String result = TimestampConverter.databaseTableString("serverName.databaseName.tableName.Envelope");
        assertEquals("databaseName.tableName",result);
    }






}

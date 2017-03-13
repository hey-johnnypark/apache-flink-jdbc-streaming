import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

import java.io.IOException;

/**
 * Created by johnnypark on 13/03/2017.
 */
public class ByteSerialisationSchema implements SerializationSchema<byte[]>, DeserializationSchema<byte[]> {

    @Override
    public byte[] serialize(byte[] bytes) {
        return bytes;
    }

    @Override
    public byte[] deserialize(byte[] bytes) throws IOException {
        return bytes;
    }

    @Override
    public boolean isEndOfStream(byte[] bytes) {
        return false;
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return TypeInformation.of(byte[].class);
    }
}

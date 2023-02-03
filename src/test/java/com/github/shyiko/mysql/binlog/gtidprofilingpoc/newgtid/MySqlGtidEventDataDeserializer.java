/*
 * Copyright 2013 Patrick Prasse
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid;

import com.github.shyiko.mysql.binlog.event.deserialization.EventDataDeserializer;
import com.github.shyiko.mysql.binlog.gtidprofilingpoc.newgtid.gtid.Gtid;
import com.github.shyiko.mysql.binlog.io.ByteArrayInputStream;

import java.io.IOException;
import java.util.UUID;


public class MySqlGtidEventDataDeserializer implements EventDataDeserializer<MySqlGtidEventData> {

    @Override
    public MySqlGtidEventData deserialize(ByteArrayInputStream inputStream) throws IOException {
        byte flags = (byte) inputStream.readInteger(1);
        long sourceIdLeastSignificantBits = inputStream.readLong(8);
        long sourceIdMostSignificantBits = inputStream.readLong(8);
        long transactionId = inputStream.readLong(8);

        return new MySqlGtidEventData(
            new Gtid(
                new UUID(sourceIdLeastSignificantBits, sourceIdMostSignificantBits),
                transactionId
            ),
            flags
        );
    }
}

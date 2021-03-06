/*
 * Copyright (c) Microsoft. All rights reserved.
 * Licensed under the MIT license. See LICENSE file in the project root for full license information.
 */
package com.microsoft.azure.eventhubs.samples.receivebydatetime;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubRuntimeInformation;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.EventHubException;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ReceiveByDateTime {

    public static void main(String[] args)
            throws EventHubException, ExecutionException, InterruptedException, IOException {
        final String namespaceName = "----ServiceBusNamespaceName-----";
        final String eventHubName = "----EventHubName-----";
        final String sasKeyName = "-----SharedAccessSignatureKeyName-----";
        final String sasKey = "---SharedAccessSignatureKey----";
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
        final EventHubClient ehClient = EventHubClient.createFromConnectionStringSync(connStr.toString());

        final EventHubRuntimeInformation eventHubInfo = ehClient.getRuntimeInformation().get();
        final String partitionId = eventHubInfo.getPartitionIds()[0]; // get first partition's id

        final PartitionReceiver receiver = ehClient.createEpochReceiverSync(
                EventHubClient.DEFAULT_CONSUMER_GROUP_NAME,
                partitionId,
                Instant.EPOCH,
                2345);

        System.out.println("date-time receiver created...");

        try {
            int receivedCount = 0;
            while (receivedCount++ < 100) {
                receiver.receive(100).thenAccept(new Consumer<Iterable<EventData>>() {
                    public void accept(Iterable<EventData> receivedEvents) {
                        int batchSize = 0;
                        if (receivedEvents != null) {
                            for (EventData receivedEvent : receivedEvents) {
                                System.out.print(String.format("Offset: %s, SeqNo: %s, EnqueueTime: %s",
                                        receivedEvent.getSystemProperties().getOffset(),
                                        receivedEvent.getSystemProperties().getSequenceNumber(),
                                        receivedEvent.getSystemProperties().getEnqueuedTime()));

                                if (receivedEvent.getBytes() != null)
                                    System.out.println(String.format("| Message Payload: %s", new String(receivedEvent.getBytes(), Charset.defaultCharset())));
                                batchSize++;
                            }
                        }

                        System.out.println(String.format("ReceivedBatch Size: %s", batchSize));
                    }
                }).get();
            }
        } finally {
            // cleaning up receivers is paramount;
            // max number of concurrent receivers per consumergroup per partition is 5
            receiver.close().whenComplete(new BiConsumer<Void, Throwable>() {
                public void accept(Void t, Throwable u) {
                    if (u != null) {
                        // wire-up this error to diagnostics infrastructure
                        System.out.println(String.format("closing failed with error: %s", u.toString()));
                    }
                    try {
                        ehClient.closeSync();
                    } catch (EventHubException sbException) {
                        // wire-up this error to diagnostics infrastructure
                        System.out.println(String.format("closing failed with error: %s", sbException.toString()));
                    }
                }
            }).get();
        }
    }

}

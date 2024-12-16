/*
 * Copyright (c) 2024 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.cdk.load.task.implementor

import com.google.common.collect.Range
import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.message.BatchEnvelope
import io.airbyte.cdk.load.message.DestinationFile
import io.airbyte.cdk.load.message.MessageQueue
import io.airbyte.cdk.load.message.MultiProducerChannel
import io.airbyte.cdk.load.state.SyncManager
import io.airbyte.cdk.load.task.DestinationTaskLauncher
import io.airbyte.cdk.load.task.ImplementorScope
import io.airbyte.cdk.load.util.use
import io.airbyte.cdk.load.write.BatchAccumulator
import io.github.oshai.kotlinlogging.KotlinLogging
import io.micronaut.context.annotation.Secondary
import jakarta.inject.Named
import jakarta.inject.Singleton
import java.io.File
import java.util.concurrent.ConcurrentHashMap

interface ProcessFileTask : ImplementorScope

class DefaultProcessFileTask(
    private val syncManager: SyncManager,
    private val taskLauncher: DestinationTaskLauncher,
    private val inputQueue: MessageQueue<FileTransferQueueMessage>,
    private val outputQueue: MultiProducerChannel<BatchEnvelope<*>>,
) : ProcessFileTask {
    val log = KotlinLogging.logger {}
    private val accumulators = ConcurrentHashMap<DestinationStream.Descriptor, BatchAccumulator>()

    override suspend fun execute() {
        outputQueue.use {
            inputQueue.consume().collect { (streamDescriptor, file, index) ->
                log.error { "consuming index $index" }
                val streamLoader = syncManager.getOrAwaitStreamLoader(streamDescriptor)

                val acc = accumulators.getOrPut(streamDescriptor) {
                    streamLoader.createBatchAccumulator(true)
                }

                val localFile = File(file.fileMessage.fileUrl)
                val fileInputStream = localFile.inputStream()

                var partCount = 0L
                while (true) {
                    val bytePart = ByteArray(1024 * 1024 * 10)
                    val read = fileInputStream.read(bytePart)

                    if (read == -1) {
                        handleFilePart(file, bytePart, partCount, true, acc, streamDescriptor, index)
                        log.error { "end of file" }
                        break
                    } else if (read < bytePart.size) {
                        handleFilePart(file, bytePart, partCount, true, acc, streamDescriptor, index)
                        log.error { "end of file" }
                        break
                    } else {
                        handleFilePart(file, bytePart, partCount, false, acc, streamDescriptor, index)
                        partCount++
                    }
                }
                localFile.delete()
            }
            log.error { "Closing input queue" }
        }
    }

    private suspend fun handleFilePart(file: DestinationFile,
                                       bytePart: ByteArray,
                                       partCount: Long,
                                       endOfStream: Boolean, acc: BatchAccumulator,
                                       streamDescriptor: DestinationStream.Descriptor,
                                       index: Long,) {
        val batch = acc.processFilePart(file, bytePart, partCount, endOfStream)
        val wrapped = BatchEnvelope(batch, Range.singleton(index), streamDescriptor)
        taskLauncher.handleNewBatch(streamDescriptor, wrapped)
        if (batch.requiresProcessing) {
            outputQueue.publish(wrapped)
        }

    }
}


interface ProcessFileTaskFactory {
    fun make(
        taskLauncher: DestinationTaskLauncher,
    ): ProcessFileTask
}

@Singleton
@Secondary
class DefaultFileRecordsTaskFactory(
    private val syncManager: SyncManager,
    @Named("fileMessageQueue") private val fileTransferQueue: MessageQueue<FileTransferQueueMessage>,
    @Named("batchQueue") private val outputQueue: MultiProducerChannel<BatchEnvelope<*>>,
) : ProcessFileTaskFactory {
    override fun make(
        taskLauncher: DestinationTaskLauncher,
    ): ProcessFileTask {
        return DefaultProcessFileTask(syncManager, taskLauncher, fileTransferQueue, outputQueue)
    }
}

data class FileTransferQueueMessage(
    val streamDescriptor: DestinationStream.Descriptor,
    val file: DestinationFile,
    val index: Long,
)

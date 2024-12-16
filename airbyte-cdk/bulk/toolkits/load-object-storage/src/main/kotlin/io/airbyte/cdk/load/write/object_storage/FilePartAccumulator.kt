package io.airbyte.cdk.load.write.object_storage

import io.airbyte.cdk.load.command.DestinationStream
import io.airbyte.cdk.load.file.object_storage.ObjectStoragePathFactory
import io.airbyte.cdk.load.file.object_storage.PartFactory
import io.airbyte.cdk.load.message.Batch
import io.airbyte.cdk.load.message.DestinationFile
import io.airbyte.cdk.load.message.object_storage.LoadablePart
import io.airbyte.cdk.load.write.BatchAccumulator
import java.nio.file.Path

class FilePartAccumulator(
    private val pathFactory: ObjectStoragePathFactory,
    private val stream: DestinationStream,
): BatchAccumulator {
    override suspend fun processFilePart(file: DestinationFile, filePart: ByteArray, partCount: Long, endOfStream: Boolean): Batch {
        val key =
            Path.of(pathFactory.getFinalDirectory(stream), "${file.fileMessage.fileRelativePath}")
                .toString()

        val part = PartFactory(
            key = key,
            fileNumber = partCount,
        )

        return LoadablePart(part.nextPart(filePart, isFinal = endOfStream))
    }
}

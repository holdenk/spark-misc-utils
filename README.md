# spark-misc-utils
Misc Utils for Spark & Big Data

## Compressed Encoder / AltEncoder

If you have sparse data which when Spark goes to serialize blows up, one option is to use a compressed encoder. The AltEncoder.COMPRESSED_BINARY can be used to handle byte arrays with sparse data. See `AltEncoderSuite.scala` for sample usage.

## Apache Iceberg Bad File Table Cleanup

Apache Iceberg does not generally perform validation of the files that it's metadata points to. This is not a problem when all clients of a table are well behaved (e.g. the standard clients), but sometimes custom clients or errors can occur. In that case you may get errors like `File does not exist.`. The Apache Iceberg Bad File Table Cleanup tool can be used to drop these invalid entries from the metadata.

At present the tool only checks that the files exist and are loadable, but PRs are welcome for things like schema validation.

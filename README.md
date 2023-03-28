# Tiered Storage for Apache Kafka®

This project is a collection of `RemoteStorageManagers` for Apache Kafka tiered storage.

## Current development

The project follows the API specifications according to the latest version of [KIP-405: Kafka Tiered Storage](https://cwiki.apache.org/confluence/x/KJDQBQ).

Currently, support for `S3` storage is built.

## Future plans

We intend to add support for `GCS` storage in the future, along with other cloud provider storage solutions.

## Design

### Requirements

Conceptually, `RemoteStorageManagers` is very simple and exposes a CRUD interface for uploading, (ranged) fetching, and deleting Kafka log segment files. This implementation was done with few additional requirements:
1. Compression. It must provide optional (configurable) compression. The compression must be conditional and double compression of Kafka-compressed log segments should be avoided.
2. Encryption. It must provide optional (configurable) encryption with key rotation support.
3. Small download overhead. It must avoid wasted download from remote storage.

### Chunking

Compression and encryption make it difficult to do ranged queries. To read an encrypted file from the middle, one needs to decompress / decrypt--and so download--the whole prefix. As files can be large, this may lead to huge download overhead.

One way to combat this is chunking. With chunking, the original file is split into chunks, which are transformed (compressed, encrypted) independently.

```
Original file:               Transformed file:
+-----------+ -------\
|           |         \
|  Chunk 1  |          \----- +-----------+
|           |                 |  Chunk 1  |
+-----------+ --------------- +-----------+
|           |                 |  Chunk 2  |
|  Chunk 2  |        /------- +-----------+
|           |       /         |  Chunk 3  |
+-----------+ -----/     /--- +-----------+
|  Chunk 3  |           /
+-----------+ ---------/
```

Now, knowing the sizes of original and transformed chunks, it's possible to map from a position in the original file `Po` to a position in the transformed file `Pt` within the accuracy of a chunk size. Thus, some overhead remains, but it is reduced as much as chunks are smaller than the whole file.

#### Concatenation and indexing

Splitting a file into multiple chunks may significantly increase the overhead of remote storage operations--most notably writes and lists--which are usually billed for. To avoid this, transformed chunks could be concatenated back into a single blob and uploaded as one.

To maintain the ability to map offsets/chunks between original and transformed files, an index is needed.

The index must be stored on the remote storage along with other files related to a log segment to keep these uploads self-sufficient.

It's important to keep the size of these indices small. One technique is to keep the size of original chunks fixed and explicitly list only the transformed side of chunks. Furthermore, if compression is not used, the size of transformed chunks also remains fixed.

##### Index encoding

Non-trivial binary encoding could reduce the index size further.

TBD

### Compression

TBD

#### Compression heuristics

TBD

#### On compression dictionaries

TBD

### Encryption

TBD

#### Key rotation

TBD

### Ranged queries

TBD

### Local cache

TBD

## License

The project is licensed under the Apache license, version 2.0. Full license test is available in the [LICENSE](LICENSE) file.

## Contributing

For a detailed guide on how to contribute to the project, please check [CONTRIBUTING.md](CONTRIBUTING.md).

## Code of conduct

This project uses the [Contributor Covenant](https://www.contributor-covenant.org/) code of conduct. Please check [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md) for further details.

## Trademark notices

Apache Kafka is either a registered trademark or trademark of the Apache Software Foundation in the United States and/or other countries.
All product and service names used in this page are for identification purposes only and do not imply endorsement.

## Credits

This project is maintained by, [Aiven](https://aiven.io/) open source developers.

Recent contributors are listed on the GitHub project page, <https://github.com/aiven/tiered-storage-for-apache-kafka/graphs/contributors>.

Copyright (c) 2022 Aiven Oy and project contributors.

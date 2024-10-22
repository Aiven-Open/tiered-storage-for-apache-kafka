=================
Core components metrics
=================

-----------------
RemoteStorageManager metrics
-----------------

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics
====================================================================

====================================  ===============================================================================================
Attribute name                        Description                                                                                    
====================================  ===============================================================================================
object-upload-bytes-rate              Rate of bytes uploaded to a storage backend                                                    
object-upload-bytes-total             Total number of bytes uploaded to a storage backend                                            
object-upload-rate                    Rate of upload to a storage backend operations                                                 
object-upload-total                   Total number of upload to a storage backend operations                                         
segment-copy-time-avg                 Average time spent processing and uploading a log segment and indexes                          
segment-copy-time-max                 Maximum time spent processing and uploading a log segment and indexes                          
segment-delete-bytes-total            Total number of deleted number of bytes estimated from segment size                            
segment-delete-errors-rate            Rate of errors during remote log segment deletion                                              
segment-delete-errors-total           Total number of errors during remote log segment deletion                                      
segment-delete-rate                   Rate of delete remote segment operations, including all its objects                            
segment-delete-time-avg               Average time spent deleting log segment and indexes                                            
segment-delete-time-max               Maximum time spent deleting log segment and indexes                                            
segment-delete-total                  Total number of delete remote segment operations, including all its objects                    
segment-fetch-requested-bytes-rate    Rate of bytes requested by broker, not necessarily the amount to be consumed by fetcher        
segment-fetch-requested-bytes-total   Total number of bytes requested by broker, not necessarily the amount to be consumed by fetcher
====================================  ===============================================================================================

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,object-type="{object-type}"
================================================================================================

==========================  ============================================================================
Attribute name              Description                                                                 
==========================  ============================================================================
object-upload-bytes-rate    Rate of bytes uploaded to a storage backend tagged by object type           
object-upload-bytes-total   Total number of bytes uploaded to a storage backend tagged by object type   
object-upload-rate          Rate of upload to a storage backend operations tagged by object type        
object-upload-total         Total number of upload to a storage backend operations tagged by object type
==========================  ============================================================================

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}"
====================================================================================

====================================  ===============================================================================================================
Attribute name                        Description                                                                                                    
====================================  ===============================================================================================================
object-upload-bytes-rate              Rate of bytes uploaded to a storage backend tagged by topic                                                    
object-upload-bytes-total             Total number of bytes uploaded to a storage backend tagged by topic                                            
object-upload-rate                    Rate of upload to a storage backend operations tagged by topic                                                 
object-upload-total                   Total number of upload to a storage backend operations tagged by topic                                         
segment-copy-time-avg                 Average time spent processing and uploading a log segment and indexes tagged by topic                          
segment-copy-time-max                 Maximum time spent processing and uploading a log segment and indexes tagged by topic                          
segment-delete-bytes-total            Total number of deleted number of bytes estimated from segment size tagged by topic                            
segment-delete-errors-rate            Rate of errors during remote log segment deletion tagged by topic                                              
segment-delete-errors-total           Total number of errors during remote log segment deletion tagged by topic                                      
segment-delete-rate                   Rate of delete remote segment operations, including all its objects tagged by topic                            
segment-delete-time-avg               Average time spent deleting log segment and indexes tagged by topic                                            
segment-delete-time-max               Maximum time spent deleting log segment and indexes tagged by topic                                            
segment-delete-total                  Total number of delete remote segment operations, including all its objects tagged by topic                    
segment-fetch-requested-bytes-rate    Rate of bytes requested by broker, not necessarily the amount to be consumed by fetcher tagged by topic        
segment-fetch-requested-bytes-total   Total number of bytes requested by broker, not necessarily the amount to be consumed by fetcher tagged by topic
====================================  ===============================================================================================================

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}",object-type="{object-type}"
================================================================================================================

==========================  ======================================================================================
Attribute name              Description                                                                           
==========================  ======================================================================================
object-upload-bytes-rate    Rate of bytes uploaded to a storage backend tagged by topic and object type           
object-upload-bytes-total   Total number of bytes uploaded to a storage backend tagged by topic and object type   
object-upload-rate          Rate of upload to a storage backend operations tagged by topic and object type        
object-upload-total         Total number of upload to a storage backend operations tagged by topic and object type
==========================  ======================================================================================

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}",partition="{partition}"
============================================================================================================

====================================  =============================================================================================================================
Attribute name                        Description                                                                                                                  
====================================  =============================================================================================================================
object-upload-bytes-rate              Rate of bytes uploaded to a storage backend tagged by topic and partition                                                    
object-upload-bytes-total             Total number of bytes uploaded to a storage backend tagged by topic and partition                                            
object-upload-rate                    Rate of upload to a storage backend operations tagged by topic and partition                                                 
object-upload-total                   Total number of upload to a storage backend operations tagged by topic and partition                                         
segment-copy-time-avg                 Average time spent processing and uploading a log segment and indexes tagged by topic and partition                          
segment-copy-time-max                 Maximum time spent processing and uploading a log segment and indexes tagged by topic and partition                          
segment-delete-bytes-total            Total number of deleted number of bytes estimated from segment size tagged by topic and partition                            
segment-delete-errors-rate            Rate of errors during remote log segment deletion tagged by topic and partition                                              
segment-delete-errors-total           Total number of errors during remote log segment deletion tagged by topic and partition                                      
segment-delete-rate                   Rate of delete remote segment operations, including all its objects tagged by topic and partition                            
segment-delete-time-avg               Average time spent deleting log segment and indexes tagged by topic and partition                                            
segment-delete-time-max               Maximum time spent deleting log segment and indexes tagged by topic and partition                                            
segment-delete-total                  Total number of delete remote segment operations, including all its objects tagged by topic and partition                    
segment-fetch-requested-bytes-rate    Rate of bytes requested by broker, not necessarily the amount to be consumed by fetcher tagged by topic and partition        
segment-fetch-requested-bytes-total   Total number of bytes requested by broker, not necessarily the amount to be consumed by fetcher tagged by topic and partition
====================================  =============================================================================================================================

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}",partition="{partition}",object-type="{object-type}"
========================================================================================================================================

==========================  ==============================================================================================
Attribute name              Description                                                                                   
==========================  ==============================================================================================
object-upload-bytes-rate    Rate of bytes uploaded to a storage backend tagged by topic, partition and object type        
object-upload-bytes-total   Total number of bytes uploaded to a storage backend tagged by topic, partition and object type
object-upload-rate          Rate of upload to a storage backend operations tagged by topic, partition and object type     
object-upload-total         Rate of upload to a storage backend operations tagged by topic, partition and object type     
==========================  ==============================================================================================



-----------------
SegmentManifestCache metrics
-----------------

aiven.kafka.server.tieredstorage.cache:type=segment-manifest-cache-metrics
==========================================================================

==============================  ========================================
Attribute name                  Description                             
==============================  ========================================
cache-eviction-total            Eviction of an entry from the cache     
cache-eviction-weight-total     Weight of evicted entry                 
cache-hits-total                Cache hits                              
cache-load-failure-time-total   Time when failing to load a new entry   
cache-load-failure-total        Failures to load a new entry            
cache-load-success-time-total   Time to load a new entry                
cache-load-success-total        Successful load of a new entry          
cache-misses-total              Cache misses                            
cache-size-total                Estimated number of entries in the cache
==============================  ========================================

aiven.kafka.server.tieredstorage.cache:type=segment-manifest-cache-metrics,cause="{cause}"
==========================================================================================

============================  ===================================================
Attribute name                Description                                        
============================  ===================================================
cache-eviction-total          Eviction of an entry from the cache tagged by cause
cache-eviction-weight-total   Weight of evicted entry tagged by cause            
============================  ===================================================



aiven.kafka.server.tieredstorage.thread-pool:type=segment-manifest-cache-thread-pool-metrics
============================================================================================

===========================  ========================================================================================================
Attribute name               Description                                                                                             
===========================  ========================================================================================================
active-thread-count-total    Number of threads currently executing tasks                                                             
parallelism-total            Targeted parallelism level of the pool                                                                  
pool-size-total              Current number of threads in the pool                                                                   
queued-task-count-total      Tasks submitted to the pool that have not yet begun executing.                                          
running-thread-count-total   Number of worker threads that are not blocked waiting to join tasks or for other managed synchronization
steal-task-count-total       Number of tasks stolen from one thread's work queue by another                                          
===========================  ========================================================================================================



-----------------
SegmentIndexesCache metrics
-----------------
aiven.kafka.server.tieredstorage.cache:type=segment-indexes-cache-metrics
=========================================================================

==============================  ========================================
Attribute name                  Description                             
==============================  ========================================
cache-eviction-total            Eviction of an entry from the cache     
cache-eviction-weight-total     Weight of evicted entry                 
cache-hits-total                Cache hits                              
cache-load-failure-time-total   Time when failing to load a new entry   
cache-load-failure-total        Failures to load a new entry            
cache-load-success-time-total   Time to load a new entry                
cache-load-success-total        Successful load of a new entry          
cache-misses-total              Cache misses                            
cache-size-total                Estimated number of entries in the cache
==============================  ========================================

aiven.kafka.server.tieredstorage.cache:type=segment-indexes-cache-metrics,cause="{cause}"
=========================================================================================

============================  ===================================================
Attribute name                Description                                        
============================  ===================================================
cache-eviction-total          Eviction of an entry from the cache tagged by cause
cache-eviction-weight-total   Weight of evicted entry tagged by cause            
============================  ===================================================


aiven.kafka.server.tieredstorage.thread-pool:type=segment-indexes-cache-thread-pool-metrics
===========================================================================================

===========================  ========================================================================================================
Attribute name               Description                                                                                             
===========================  ========================================================================================================
active-thread-count-total    Number of threads currently executing tasks                                                             
parallelism-total            Targeted parallelism level of the pool                                                                  
pool-size-total              Current number of threads in the pool                                                                   
queued-task-count-total      Tasks submitted to the pool that have not yet begun executing.                                          
running-thread-count-total   Number of worker threads that are not blocked waiting to join tasks or for other managed synchronization
steal-task-count-total       Number of tasks stolen from one thread's work queue by another                                          
===========================  ========================================================================================================



-----------------
ChunkCache metrics
-----------------

aiven.kafka.server.tieredstorage.cache:type=chunk-cache-metrics
===============================================================

==============================  ========================================
Attribute name                  Description                             
==============================  ========================================
cache-eviction-total            Eviction of an entry from the cache     
cache-eviction-weight-total     Weight of evicted entry                 
cache-hits-total                Cache hits                              
cache-load-failure-time-total   Time when failing to load a new entry   
cache-load-failure-total        Failures to load a new entry            
cache-load-success-time-total   Time to load a new entry                
cache-load-success-total        Successful load of a new entry          
cache-misses-total              Cache misses                            
cache-size-total                Estimated number of entries in the cache
==============================  ========================================

aiven.kafka.server.tieredstorage.cache:type=chunk-cache-metrics,cause="{cause}"
===============================================================================

============================  ===================================================
Attribute name                Description                                        
============================  ===================================================
cache-eviction-total          Eviction of an entry from the cache tagged by cause
cache-eviction-weight-total   Weight of evicted entry tagged by cause            
============================  ===================================================



aiven.kafka.server.tieredstorage.thread-pool:type=chunk-cache-thread-pool-metrics
=================================================================================

===========================  ========================================================================================================
Attribute name               Description                                                                                             
===========================  ========================================================================================================
active-thread-count-total    Number of threads currently executing tasks                                                             
parallelism-total            Targeted parallelism level of the pool                                                                  
pool-size-total              Current number of threads in the pool                                                                   
queued-task-count-total      Tasks submitted to the pool that have not yet begun executing.                                          
running-thread-count-total   Number of worker threads that are not blocked waiting to join tasks or for other managed synchronization
steal-task-count-total       Number of tasks stolen from one thread's work queue by another                                          
===========================  ========================================================================================================



=================
Storage Backend metrics
=================

-----------------
AzureBlobStorage metrics
-----------------

aiven.kafka.server.tieredstorage.azure:type=azure-blob-storage-client-metrics
=============================================================================

========================  ============================================================
Attribute name            Description                                                 
========================  ============================================================
blob-delete-rate          Rate of object delete operations                            
blob-delete-total         Total number of object delete operations                    
blob-get-rate             Rate of get object operations                               
blob-get-total            Total number of get object operations                       
blob-upload-rate          Rate of object upload operations                            
blob-upload-total         Total number of object upload operations                    
block-list-upload-rate    Rate of block list (making a blob) upload operations        
block-list-upload-total   Total number of block list (making a blob) upload operations
block-upload-rate         Rate of block (blob part) upload operations                 
block-upload-total        Total number of block (blob part) upload operations         
========================  ============================================================



-----------------
GcsStorage metrics
-----------------

aiven.kafka.server.tieredstorage.gcs:type=gcs-client-metrics
============================================================

================================  ===================================================================
Attribute name                    Description                                                        
================================  ===================================================================
object-delete-rate                Rate of delete object operations                                   
object-delete-total               Total number of delete object operations                           
object-get-rate                   Rate of get object operations                                      
object-get-total                  Total number of get object operations                              
object-metadata-get-rate          Rate of get object metadata operations                             
object-metadata-get-total         Total number of get object metadata operations                     
resumable-chunk-upload-rate       Rate of upload chunk operations as part of resumable upload        
resumable-chunk-upload-total      Total number of upload chunk operations as part of resumable upload
resumable-upload-initiate-rate    Rate of initiate resumable upload operations                       
resumable-upload-initiate-total   Total number of initiate resumable upload operations               
================================  ===================================================================



-----------------
S3Storage metrics
-----------------

aiven.kafka.server.tieredstorage.s3:type=s3-client-metrics
==========================================================

=========================================  =============================================================================
Attribute name                             Description                                                                  
=========================================  =============================================================================
abort-multipart-upload-requests-rate       Rate of abort multi-part upload operations                                   
abort-multipart-upload-requests-total      Total number of abort multi-part upload operations                           
abort-multipart-upload-time-avg            Average time spent aborting a new multi-part upload operation                
abort-multipart-upload-time-max            Maximum time spent aborting a new multi-part upload operation                
complete-multipart-upload-requests-rate    Rate of complete multi-part upload operations                                
complete-multipart-upload-requests-total   Total number of complete multi-part upload operations                        
complete-multipart-upload-time-avg         Average time spent completing a new multi-part upload operation              
complete-multipart-upload-time-max         Maximum time spent completing a new multi-part upload operation              
configured-timeout-errors-rate             Rate of configured timeout errors                                            
configured-timeout-errors-total            Total number of configured timeout errors                                    
create-multipart-upload-requests-rate      Rate of create multi-part upload operations                                  
create-multipart-upload-requests-total     Total number of create multi-part upload operations                          
create-multipart-upload-time-avg           Average time spent creating a new multi-part upload operation                
create-multipart-upload-time-max           Maximum time spent creating a new multi-part upload operation                
delete-object-requests-rate                Rate of delete object request operations                                     
delete-object-requests-total               Total number of delete object request operations                             
delete-object-time-avg                     Average time spent deleting an object                                        
delete-object-time-max                     Maximum time spent deleting an object                                        
delete-objects-requests-rate               Rate of delete a set of objects request operations                           
delete-objects-requests-total              Total number of delete a set of objects request operations                   
delete-objects-time-avg                    Average time spent deleting a set of objects                                 
delete-objects-time-max                    Maximum time spent deleting a set of objects                                 
get-object-requests-rate                   Rate of get object request operations                                        
get-object-requests-total                  Total number of get object request operations                                
get-object-time-avg                        Average time spent getting a response from a get object request              
get-object-time-max                        Maximum time spent getting a response from a get object request              
io-errors-rate                             Rate of IO errors                                                            
io-errors-total                            Total number of IO errors                                                    
other-errors-rate                          Rate of other errors                                                         
other-errors-total                         Total number of other errors                                                 
put-object-requests-rate                   Rate of put object request operations                                        
put-object-requests-total                  Total number of put object request operations                                
put-object-time-avg                        Average time spent uploading an object                                       
put-object-time-max                        Maximum time spent uploading an object                                       
server-errors-rate                         Rate of server errors                                                        
server-errors-total                        Total number of server errors                                                
throttling-errors-rate                     Rate of throttling errors                                                    
throttling-errors-total                    Total number of throttling errors                                            
upload-part-requests-rate                  Rate of upload part request operations (as part of multi-part upload)        
upload-part-requests-total                 Total number of upload part request operations (as part of multi-part upload)
upload-part-time-avg                       Average time spent uploading a single part                                   
upload-part-time-max                       Maximum time spent uploading a single part                                   
=========================================  =============================================================================



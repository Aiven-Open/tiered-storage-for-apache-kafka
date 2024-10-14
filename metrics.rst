=================
Core components metrics
=================

-----------------
RemoteStorageManager metrics
-----------------

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics
====================================================================

====================================  ===========
Attribute name                        Description
====================================  ===========
object-upload-bytes-rate                         
object-upload-bytes-total                        
object-upload-rate                               
object-upload-total                              
segment-copy-time-avg                            
segment-copy-time-max                            
segment-delete-bytes-total                       
segment-delete-errors-rate                       
segment-delete-errors-total                      
segment-delete-rate                              
segment-delete-time-avg                          
segment-delete-time-max                          
segment-delete-total                             
segment-fetch-requested-bytes-rate               
segment-fetch-requested-bytes-total              
====================================  ===========

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,object-type="{object-type}"
================================================================================================

==========================  ===========
Attribute name              Description
==========================  ===========
object-upload-bytes-rate               
object-upload-bytes-total              
object-upload-rate                     
object-upload-total                    
==========================  ===========

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}"
====================================================================================

====================================  ===========
Attribute name                        Description
====================================  ===========
object-upload-bytes-rate                         
object-upload-bytes-total                        
object-upload-rate                               
object-upload-total                              
segment-copy-time-avg                            
segment-copy-time-max                            
segment-delete-bytes-total                       
segment-delete-errors-rate                       
segment-delete-errors-total                      
segment-delete-rate                              
segment-delete-time-avg                          
segment-delete-time-max                          
segment-delete-total                             
segment-fetch-requested-bytes-rate               
segment-fetch-requested-bytes-total              
====================================  ===========

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}",object-type="{object-type}"
================================================================================================================

==========================  ===========
Attribute name              Description
==========================  ===========
object-upload-bytes-rate               
object-upload-bytes-total              
object-upload-rate                     
object-upload-total                    
==========================  ===========

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}",partition="{partition}"
============================================================================================================

====================================  ===========
Attribute name                        Description
====================================  ===========
object-upload-bytes-rate                         
object-upload-bytes-total                        
object-upload-rate                               
object-upload-total                              
segment-copy-time-avg                            
segment-copy-time-max                            
segment-delete-bytes-total                       
segment-delete-errors-rate                       
segment-delete-errors-total                      
segment-delete-rate                              
segment-delete-time-avg                          
segment-delete-time-max                          
segment-delete-total                             
segment-fetch-requested-bytes-rate               
segment-fetch-requested-bytes-total              
====================================  ===========

aiven.kafka.server.tieredstorage:type=remote-storage-manager-metrics,topic="{topic}",partition="{partition}",object-type="{object-type}"
========================================================================================================================================

==========================  ===========
Attribute name              Description
==========================  ===========
object-upload-bytes-rate               
object-upload-bytes-total              
object-upload-rate                     
object-upload-total                    
==========================  ===========



-----------------
SegmentManifestCache metrics
-----------------

aiven.kafka.server.tieredstorage.cache:type=segment-manifest-cache-metrics
==========================================================================

==============================  ===========
Attribute name                  Description
==============================  ===========
cache-eviction-total                       
cache-eviction-weight-total                
cache-hits-total                           
cache-load-failure-time-total              
cache-load-failure-total                   
cache-load-success-time-total              
cache-load-success-total                   
cache-misses-total                         
cache-size-total                           
==============================  ===========

aiven.kafka.server.tieredstorage.cache:type=segment-manifest-cache-metrics,cause="{cause}"
==========================================================================================

============================  ===========
Attribute name                Description
============================  ===========
cache-eviction-total                     
cache-eviction-weight-total              
============================  ===========



aiven.kafka.server.tieredstorage.thread-pool:type=segment-manifest-cache-thread-pool-metrics
============================================================================================

===========================  ===========
Attribute name               Description
===========================  ===========
active-thread-count-total               
parallelism-total                       
pool-size-total                         
queued-task-count-total                 
running-thread-count-total              
steal-task-count-total                  
===========================  ===========



-----------------
SegmentIndexesCache metrics
-----------------
aiven.kafka.server.tieredstorage.cache:type=segment-indexes-cache-metrics
=========================================================================

==============================  ===========
Attribute name                  Description
==============================  ===========
cache-eviction-total                       
cache-eviction-weight-total                
cache-hits-total                           
cache-load-failure-time-total              
cache-load-failure-total                   
cache-load-success-time-total              
cache-load-success-total                   
cache-misses-total                         
cache-size-total                           
==============================  ===========

aiven.kafka.server.tieredstorage.cache:type=segment-indexes-cache-metrics,cause="{cause}"
=========================================================================================

============================  ===========
Attribute name                Description
============================  ===========
cache-eviction-total                     
cache-eviction-weight-total              
============================  ===========


aiven.kafka.server.tieredstorage.thread-pool:type=segment-indexes-cache-thread-pool-metrics
===========================================================================================

===========================  ===========
Attribute name               Description
===========================  ===========
active-thread-count-total               
parallelism-total                       
pool-size-total                         
queued-task-count-total                 
running-thread-count-total              
steal-task-count-total                  
===========================  ===========



-----------------
ChunkCache metrics
-----------------

aiven.kafka.server.tieredstorage.cache:type=chunk-cache-metrics
===============================================================

==============================  ===========
Attribute name                  Description
==============================  ===========
cache-eviction-total                       
cache-eviction-weight-total                
cache-hits-total                           
cache-load-failure-time-total              
cache-load-failure-total                   
cache-load-success-time-total              
cache-load-success-total                   
cache-misses-total                         
cache-size-total                           
==============================  ===========

aiven.kafka.server.tieredstorage.cache:type=chunk-cache-metrics,cause="{cause}"
===============================================================================

============================  ===========
Attribute name                Description
============================  ===========
cache-eviction-total                     
cache-eviction-weight-total              
============================  ===========



aiven.kafka.server.tieredstorage.thread-pool:type=chunk-cache-thread-pool-metrics
=================================================================================

===========================  ===========
Attribute name               Description
===========================  ===========
active-thread-count-total               
parallelism-total                       
pool-size-total                         
queued-task-count-total                 
running-thread-count-total              
steal-task-count-total                  
===========================  ===========



=================
Storage Backend metrics
=================

-----------------
AzureBlobStorage metrics
-----------------

aiven.kafka.server.tieredstorage.azure:type=azure-blob-storage-client-metrics
=============================================================================

========================  ===========
Attribute name            Description
========================  ===========
blob-delete-rate                     
blob-delete-total                    
blob-get-rate                        
blob-get-total                       
blob-upload-rate                     
blob-upload-total                    
block-list-upload-rate               
block-list-upload-total              
block-upload-rate                    
block-upload-total                   
========================  ===========



-----------------
GcsStorage metrics
-----------------

aiven.kafka.server.tieredstorage.gcs:type=gcs-client-metrics
============================================================

================================  ===========
Attribute name                    Description
================================  ===========
object-delete-rate                           
object-delete-total                          
object-get-rate                              
object-get-total                             
object-metadata-get-rate                     
object-metadata-get-total                    
resumable-chunk-upload-rate                  
resumable-chunk-upload-total                 
resumable-upload-initiate-rate               
resumable-upload-initiate-total              
================================  ===========



-----------------
S3Storage metrics
-----------------

aiven.kafka.server.tieredstorage.s3:type=s3-client-metrics
==========================================================

=========================================  ===========
Attribute name                             Description
=========================================  ===========
abort-multipart-upload-requests-rate                  
abort-multipart-upload-requests-total                 
abort-multipart-upload-time-avg                       
abort-multipart-upload-time-max                       
complete-multipart-upload-requests-rate               
complete-multipart-upload-requests-total              
complete-multipart-upload-time-avg                    
complete-multipart-upload-time-max                    
configured-timeout-errors-rate                        
configured-timeout-errors-total                       
create-multipart-upload-requests-rate                 
create-multipart-upload-requests-total                
create-multipart-upload-time-avg                      
create-multipart-upload-time-max                      
delete-object-requests-rate                           
delete-object-requests-total                          
delete-object-time-avg                                
delete-object-time-max                                
delete-objects-requests-rate                          
delete-objects-requests-total                         
delete-objects-time-avg                               
delete-objects-time-max                               
get-object-requests-rate                              
get-object-requests-total                             
get-object-time-avg                                   
get-object-time-max                                   
io-errors-rate                                        
io-errors-total                                       
other-errors-rate                                     
other-errors-total                                    
put-object-requests-rate                              
put-object-requests-total                             
put-object-time-avg                                   
put-object-time-max                                   
server-errors-rate                                    
server-errors-total                                   
throttling-errors-rate                                
throttling-errors-total                               
upload-part-requests-rate                             
upload-part-requests-total                            
upload-part-time-avg                                  
upload-part-time-max                                  
=========================================  ===========



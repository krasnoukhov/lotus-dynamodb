v0.1.3
------

* Fix coercer to preserve nil attributes

v0.1.2
------

* Make Collection#update respect nil attributes
* Fix continuation & limit bug

v0.1.1
------

* Bump lotus-model dependency to 0.1.1
* Switch Lotus::Utils::Kernel.* to Lotus::Model::Mapping::Coercions.*
* Improve Query#all to return real all entries
* Improve Query#count to return real count
* Improve Query#each to iterate over batches of entries
* Improve DynamodbAdapter#clear to clear even large tables

v0.1.0
------

* Initial version

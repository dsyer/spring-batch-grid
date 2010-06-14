# Spring Batch Grid #

Aggregator for various other projects that provide grid features in Spring Batch (which might have independent release cycles).  Some of the submodules might be work in progress, while others are more mature.  If you are mainly interested in a particular grid provider, go straight to that module in its own repository.

To build, clone from git and then

    $ cd spring-batch-grid
    $ git submodule init
    $ git submodule update
    $ mvn install

Any errors should be due to problems in the submodules, so they need to be addressed there.
# check what is saved to disk on bft smart

    $ cd /home/sugo/Documents/tese/bft-smart/src/bftsmart
    $ rg -n --heading --color=always File | less

# changes for the research branch

* remove TLS from clients
* maybe replicas use non-async communication
* use MACs instead of pubkey signatures
* PRE-PREPARE messages include the request bodies, rather than
  just their hash digests
* requests are concurrently added to the request queue, and
  don't go through the master channel (use Mutex)

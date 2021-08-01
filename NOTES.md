# check what is saved to disk on bft smart

    $ cd /home/sugo/Documents/tese/bft-smart/src/bftsmart
    $ rg -n --heading --color=always File | less

# changes for the research branch

* view change `hasProof` only checks the sending node's
  signature, rather than the signature of the consensus
  messages of all nodes in the proof
* group flush() calls together, by sorting replies
  per node id
* remove TLS from clients
* maybe replicas use non-async communication
* use MACs instead of pubkey signatures
* PRE-PREPARE messages include the request bodies, rather than
  just their hash digests
* requests are concurrently added to the request queue, and
  don't go through the master channel (use Mutex)
* send_node on execution layer, so we don't need to go
  through the master channel to send replies to clients

## algorithm to perform research branch changes

1. order changes by level of complexity, from least
  difficult to most difficult
2. implement changes in this order, by creating a new
   branch `research-<feature>` starting from the previous
   change's branch
3. the first feature's branch starts from `view-change`

# systems in rust

* https://www.ibr.cs.tu-bs.de/users/ruesch/papers/ruesch-serial19.pdf
* https://crates.io/crates/overlord
* https://crates.io/crates/brb
* https://crates.io/crates/aleph-bft
    + https://github.com/Cardinal-Cryptography/AlephBFT

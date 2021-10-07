# dynamic batch size

implement dynamic instrospection of the batch size being used,
and introspect the time between each constructed batch,
to minimize request latency and maximize the system throughput

# check what is saved to disk on bft smart

    $ cd ~/Documents/tese/bft-smart/src/bftsmart
    $ rg -n --heading --color=always File | less

# check use of parallel execs

    $ rg 'submit\(\(\)' ~/Documents/tese/bft-smart/src/bftsmart

# changes done to the research branch

## to be resolved

the system appears to hang after an undeterminate amount of time,
causing the request processing to halt completely, and the
throughput to become 0

## implemented

* view change `hasProof` checks for signature of
  `ACCEPT` aka `COMMIT` messages only
    + sign only `COMMIT` messages
* speculatively create (i.e. sign) `COMMIT` msg
  before the prepared state
* group flush() calls together, by sorting replies
  per node id
* remove TLS from clients
    + do we need to check hmacs?
* send_node on execution layer, so we don't need to go
  through the master channel to send replies to clients
* `PRE-PREPARE` messages include the request bodies, rather than
  just their hash digests
    + clients should maintain a separate sequence number used
      to discard requests that have already been processed,
      the operation id as used in BFT-SMaRt
    + blindly add these requests to the log..? this may affect
      the correctness of the sound predicate from Cachin, if
      the leader is forging requests; read BFT-SMaRt code again,
      check if they consult the log for the existence of these reqs
* propose requests as soon as possible
    + pull up to `BATCH_SIZE` requests from the queue, with
      a minimum of 1, then start proposing immediately; this
      improves the request processing latency

## given up

* requests are concurrently added to the request queue, and
  don't go through the master channel (use Mutex)
* maybe replicas use non-async communication
    + BFT-SMaRt has a send thread, so we will probably
      disregard this change

## algorithm to perform research branch changes

1. order changes by level of complexity, from least
  difficult to most difficult
2. implement changes in this order, by creating a new
   branch `research-<feature>` starting from the previous
   change's branch
3. the first feature's branch starts from `view-change`
4. lastly, try to merge all the changes of `research` into
   the branch `ycsb`; fix conflicts with an interactive `git`
   command line tool

# systems in rust

* https://www.ibr.cs.tu-bs.de/users/ruesch/papers/ruesch-serial19.pdf
* https://crates.io/crates/overlord
* https://crates.io/crates/brb
* https://crates.io/crates/aleph-bft
    + https://github.com/Cardinal-Cryptography/AlephBFT

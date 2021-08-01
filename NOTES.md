# check what is saved to disk on bft smart

    $ cd /home/sugo/Documents/tese/bft-smart/src/bftsmart
    $ rg -n --heading --color=always File | less

# changes for the research branch

* speculatively create (i.e. sign) `COMMIT` msg
  before the prepared state
* view change `hasProof` checks for signature of
  `ACCEPT` aka `COMMIT` messages only
* group flush() calls together, by sorting replies
  per node id
* remove TLS from clients
    + do we need to check hmacs?
* maybe replicas use non-async communication
* use MACs instead of pubkey signatures
* `PRE-PREPARE` messages include the request bodies, rather than
  just their hash digests
    + blindly add these requests to the log..? this may affect
      the correctness of the sound predicate from Cachin, if
      the leader is forging requests; read BFT-SMaRt code again,
      check if they consult the log for the existence of these reqs
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

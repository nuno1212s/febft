# improvements to current febft implementation

* `[x]` improve map hashing algorithm
    + solved: use **fxhash**
    + <https://hg.openjdk.java.net/jdk8/jdk8/jdk/file/687fd7c7986d/src/share/classes/java/lang/String.java>
* `[x]` run profiling in `febft`
    + solved: a lot of time spent on hashmaps
* `[x]` improve tcp settings
    + [**tokio**](https://docs.rs/tokio/latest/tokio/net/struct.TcpStream.html#method.from_std)
    + [**async-std**](https://docs.rs/async-std/latest/async_std/net/struct.TcpStream.html#impl-From%3CTcpStream%3E)
    + with [**socket2**](https://docs.rs/socket2/0.4.2/socket2/struct.Socket.html#method.set_recv_buffer_size)
* `[ ]` improve testing approach
    - run without any other processes in the cluster potentially altering results
    - run tests for longer (e.g. each client sends 20,000 requests)
* `[ ]` improve communication code in `src/bft/communication/mod.rs`?
    + maybe reduce number of `.clone()` calls on buffers
        - using `Arc<Buffer>` and cloning the buf is actually slower...
        - use faster allocator? right now we are trying out `mimalloc` over the `std`
          allocator
* `[ ]` implement `io_uring` async executor
    - try out [**nuklei**](https://github.com/vertexclique/nuclei)
* `[ ]` improve map accesses
    + try out [**intmap**](https://github.com/JesperAxelsson/rust-intmap)
    + parallelize map accesses
        - do we need to synchronize each op with `.await`?
    + try using [**dashmap**](https://github.com/xacrimon/dashmap)
    + benchmarks for [**dashmap**](https://github.com/xacrimon/conc-map-bench)
* `[ ]` improve batching
    + use interpolation?
        - probably not the right approach... either way
        - calculate local maxima of first order derivate of interpolated function
          $T(t) = \frac{\texttt{measurement interval}}{\Delta t}$ where
          $\Delta t = \texttt{time now} - \texttt{time last measurement}$,
          which yields the throughput of the system at any given instant $t$
        - <https://docs.rs/interpolation/latest/interpolation/>
        - <https://www.dcode.fr/function-equation-finder>
* `[ ]` improve consensus performance
    + `microbenchmarks` reveal our `PREPARE` phase has a fair bit of latency for whatever reason

<!--
# systems in rust

* <https://www.ibr.cs.tu-bs.de/users/ruesch/papers/ruesch-serial19.pdf>
* <https://crates.io/crates/overlord>
* <https://crates.io/crates/brb>
* <https://crates.io/crates/aleph-bft>
    + <https://github.com/Cardinal-Cryptography/AlephBFT>
-->

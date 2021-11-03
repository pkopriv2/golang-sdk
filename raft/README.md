# Overview

This is an implementation of the RAFT [1] algorithm.  It allows consumers to create highly-resilient, strongly
consistent state machines, without all the hassle of managing consensus.

# Distributed Consensus

Distributed consensus is the process by which a group of machines is able to agree to a set of facts.  In the case
of Raft, the algorithm ensures that a log is consistent across a set of members.  The log has the following guarantees:

* All committed items will eventually be received by all live members
* All committed items will be received in the order they are written
* Committed items will never be lost
* Committed items will only be received once\*

\* The log guarantees exactly-once delivery at the level of items, however, this is NOT inherited by consumer entries.  
It is possible for an append operation to respond with an error but still complete.   Therefore, consumers that require
strict linearizability must also be idempotent.  

# Getting Started

Raft has been designed to be embedded in other projects as a basis for distributed consensus.  To get started, pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/golang-sdk
```

Import it:

```go
import "github.com/pkopriv2/golang-sdk/raft"
```

## Starting a Cluster

This has been built with dynamic membership as a first-class property.  Therefore, clusters are grown rather
than started.  The process for growing a cluster is done by starting a single member and then joining 
additional members as required.

To start a cluster, there must be a first member.  It's special in only that it is first.

To start a seed member, simply invoke:

```go
host, err := raft.Start(ctx, ":10240")
if err != nil {
    return err
}
```

### Adding Members

To add a member:

```go
host, err := raft.Join(ctx, ":10240", []string{"host.co.com:10240"})
if err != nil {
    return err
}
```

### Removing Members

To remove a member:

```go
host.Close()
```

## Using the Log 

Appending to the log:

```go
log, err := host.Log()
if err != nil {
    return
}

entry, err := log.Append(cancel, []string("hello, world"))
if err != nil {
    return
}
```

Compacting the log:

```go

log, err := host.Log()
if err != nil {
    return
}

entries := make(map[string]raft.Entry)
go func() {
    buf, err := log.Listen(0, 1024)
    if err != nil {
        return
    }

    for {
        select {
        case <-ctx.Closed()
            return
        case <-buf.Data():
            ... append to entries in thread-safe way ... 
        }
        return
    }
}

go func() {
    timer := time.NewTimer(30*time.Minute)
    defer timer.Stop()

    for {
        select {
        case <-ctx.Closed()
            return
        case <-timer.C:
        }

        maxIndex, ch := ... make snapshot channel ...
        
        if err := log.Compact(maxIndex, ch); err != nil {
            return
        }

        timer = time.NewTimer(30*time.Minutes)
    }
}
```

# Security

This is the earliest version of kayak and security has not been integrated.  However, 
we plan to tackle this issue shortly.  

More details to come.

# Contributors

* Preston Koprivica: pkopriv2@gmail.com

# References:

1. https://raft.github.io/raft.pdf
2. http://cs.brown.edu/~mph/HerlihyW90/p463-herlihy.pdf

# License

// TODO: 


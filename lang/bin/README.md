# Bin

Bin is a set of APIs for working with binary data.  Currently, there is only a single binary Key implementation.

## Getting Started

Pull the dependency with go get:

```sh
go get http://github.com/pkopriv2/golang-sdk
```

Import it:

```go
import "github.com/pkopriv2/golang-sdk/lang/bin"
```

### Working with Keys

Stash contains basically a single method:

```go
stash := stash.Open(ctx, "path/to/stash.db")
```

This method is guaranteed thread-safe and will only produce a single
bolt.DB instance per stash location.

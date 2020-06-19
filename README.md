# Simple producer/consumer verification


## build

```sh
go build ./...
```


## run producer with autocreate=true

This creates a topic called `sanfrancisco`

```sh
go run producer/main.go 
```


## run consumer with offset checkpointing

This will checkpoint every message offset consumed.
Stop and start multiple times to verify behavior

```sh
go run producer/main.go 
```

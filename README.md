# SqlQueue.Net
A sql-based implementation of a competitor consumer queue pattern

### Usage (SqlServer)

```CSharp
var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

queue.Enqueue(345);

int peekedElement;

long? id = queue.Peek(out peekedElement);		//this peek is exclusive between thread/processes

// do work with the element

queue.Dequeue(id.Value);		//this deletes the item from the queue
```

### TODO
 * Other database implementations.
 * Soft-delete strategy of the queue.
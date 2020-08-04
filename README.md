# SqlQueue.Net
A sql-based implementation of a competitor consumer queue pattern

### Usage (SqlServer)

```CSharp
string ConnectionString = @"Server=.\SQLEXPRESS;Database=SqlQueueTests;Integrated security=True;";

var queue = new SqlServerSqlQueue<int>(ConnectionString, "dbo", "Queue");

queue.Enqueue(345);

int peekedElement;

long? id = queue.Peek(out peekedElement);		//this peek is exclusive between thread/processes

// do work with the element

queue.Dequeue(id.Value);		//this deletes the item from the queue
```

### TODO
 * Other database implementations.
 * Soft-delete strategy of the queue.

using Newtonsoft.Json;
using NUnit.Framework;
using SqlQueue.Core.SqlServer;
using SqlQueue.Tests.Helpers;
using System;
using System.Data.SqlClient;

namespace SqlQueue.Tests.SqlServer
{
	[TestFixture]
	[Description("Test of SqlServerSqlQueue class.")]
	public class SqlServerSqlQueueTest
	{
		private const string ConnectionString = @"Server=.\SQLEXPRESS;Database=SqlQueueTests;Integrated security=True;MultipleActiveResultSets=true";
		private const string SchemaName = "dbo";
		private readonly string QueueName = "Queue";

		public SqlServerSqlQueueTest()
		{
			QueueName = QueueName + "-" + Guid.NewGuid().ToString("N").ToUpper();
		}

		[SetUp]
		public void SetUp()
		{
			try
			{
				SqlHelper.ExecuteSql(ConnectionString, $"DELETE FROM [{SchemaName}].[{QueueName}]");
			}
			catch { }
		}

		[TearDown]
		public void TearDown()
		{
			try
			{
				SqlHelper.ExecuteSql(ConnectionString, $"DELETE FROM [{SchemaName}].[{QueueName}]");
			}
			catch { }
		}

		[OneTimeTearDown]
		public void OneTimeTearDown()
		{
			try
			{
				SqlHelper.ExecuteSql(ConnectionString, $"DROP TABLE [{SchemaName}].[{QueueName}]");
			}
			catch { }
		}

		[Test]
		[Description("Tests queue creation.")]
		public void Queue_Creation_Success()
		{
			new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

			new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);  //twice for ensure no collision

			Assert.DoesNotThrow(() => SqlHelper.QuerySingle(ConnectionString, $"SELECT * FROM [{SchemaName}].[{QueueName}]"), "Queue not created correctly.");
		}

		[Test]
		[Description("Tests item enqueue.")]
		public void Queue_Enqueue_Success()
		{
			var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

			queue.Enqueue(345);

			var items = SqlHelper.Query<int>(ConnectionString, $"SELECT * FROM [{SchemaName}].[{QueueName}]", (reader) =>
			{
				return Convert.ToInt32(JsonConvert.DeserializeObject<int>(reader.GetString(4)));
			});

			Assert.AreEqual(1, items.Count, "Wrong item collection.");
			Assert.AreEqual(345, items[0], "Wrong item.");
		}

		[Test]
		[Description("Tests item enqueue in transaction.")]
		public void Queue_EnqueueInTransaction_Success()
		{
			using (var connection = new SqlConnection(ConnectionString))
			{
				connection.Open();

				using (var transaction = connection.BeginTransaction())
				{
					var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

					queue.Enqueue(345, transaction);

					int count = queue.Count();

					Assert.AreEqual(1, count, "Initial count wrong.");

					transaction.Rollback();

					count = queue.Count();

					Assert.AreEqual(0, count, "Final count wrong.");
				}
			}
		}

		[Test]
		[Description("Tests item peek.")]
		public void Queue_Peek_Success()
		{
			var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

			queue.Enqueue(345);

			var items = SqlHelper.Query<int>(ConnectionString, $"SELECT * FROM [{SchemaName}].[{QueueName}]", (reader) =>
			{
				return Convert.ToInt32(JsonConvert.DeserializeObject<int>(reader.GetString(4)));
			});

			Assert.AreEqual(1, items.Count, "Wrong item collection.");
			Assert.AreEqual(345, items[0], "Wrong item.");
			Assert.AreEqual(1, queue.Count(), "Wrong item collection by queue count.");

			int peekedElement;

			long? id = queue.Peek(out peekedElement);

			Assert.IsNotNull(id, "Id is null after peek.");
			Assert.IsTrue(id.Value > 0, "Id <= 0");
		}

		[Test]
		[Description("Tests item dequeue.")]
		public void Queue_Dequeue_Success()
		{
			var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

			queue.Enqueue(345);

			int count = queue.Count();

			Assert.AreEqual(1, count, "Initial count wrong.");

			int itemPeeked;
			long? id = queue.Peek(out itemPeeked);

			Assert.IsTrue(id > 0, "Wrong id of the peeked item.");
			Assert.AreEqual(345, itemPeeked, "Item wrong peeked.");

			queue.Dequeue(id.Value);

			count = queue.Count();

			Assert.AreEqual(0, count, "Final count wrong.");
		}

		[Test]
		[Description("Tests item dequeue in transaction.")]
		public void Queue_DequeueInTransaction_Success()
		{
			using (var connection = new SqlConnection(ConnectionString))
			{
				connection.Open();

				using (var transaction = connection.BeginTransaction())
				{
					var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

					queue.Enqueue(345);

					int count = queue.Count();

					Assert.AreEqual(1, count, "Initial count wrong.");

					int itemPeeked;
					long? id = queue.Peek(out itemPeeked);

					Assert.IsTrue(id > 0, "Wrong id of the peeked item.");
					Assert.AreEqual(345, itemPeeked, "Item wrong peeked.");

					queue.Dequeue(id.Value, transaction);

					count = queue.Count();

					Assert.AreEqual(0, count, "Wrong count after dequeue.");

					transaction.Rollback();

					count = queue.Count();

					Assert.AreEqual(1, count, "Wrong count after rollback.");
				}
			}
		}

		[Test]
		[Description("Tests basic exclusive fetch.")]
		public void Queue_Basic_ExclusiveFetch()
		{
			var queue = new SqlServerSqlQueue<int>(ConnectionString, SchemaName, QueueName);

			queue.Enqueue(1000);
			queue.Enqueue(1001);

			int count = queue.Count();

			Assert.AreEqual(2, count, "Initial count wrong.");

			using (var connection1 = new SqlConnection(ConnectionString))
			using (var connection2 = new SqlConnection(ConnectionString))
			{
				connection1.Open();
				connection2.Open();

				using (var transaction1 = connection1.BeginTransaction())
				using (var transaction2 = connection2.BeginTransaction())
				{
					int peekedElement1;

					long? id1 = queue.Peek(out peekedElement1);

					int peekedElement2;

					long? id2 = queue.Peek(out peekedElement2);

					Assert.AreEqual(1000, peekedElement1, "Element 1 wrong.");
					Assert.AreEqual(1001, peekedElement2, "Element 2 wrong.");

					queue.Dequeue(id1.Value, transaction1);
					queue.Dequeue(id2.Value, transaction2);

					count = queue.Count();

					Assert.AreEqual(0, count, "In transaction count wrong.");

					transaction1.Rollback();
					transaction2.Rollback();
				}
			}

			count = queue.Count();

			Assert.AreEqual(2, count, "Final count wrong.");
		}
	}
}
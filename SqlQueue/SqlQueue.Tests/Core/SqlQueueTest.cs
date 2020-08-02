using Newtonsoft.Json;
using NUnit.Framework;
using SqlQueue.Core;
using SqlQueue.Tests.Helpers;
using System;
using System.Transactions;

namespace SqlQueue.Tests.Core
{
	[TestFixture]
	[Description("Test of SqlQueue class.")]
	public class SqlQueueTest
	{
		private const string ConnectionString = @"Server=.\SQLEXPRESS;Database=SqlQueueTests;Integrated security=True";

		[Test]
		[Description("Tests queue creation.")]
		public void Queue_Creation_Success()
		{
			using (var transactionScope = new TransactionScope())
			{
				string queueName = "Queue";

				new SqlQueue<int>(ConnectionString, "dbo", queueName);

				new SqlQueue<int>(ConnectionString, "dbo", queueName);	//twice for ensure no collision

				Assert.DoesNotThrow(() => SqlHelper.QuerySingle(ConnectionString, $"SELECT * FROM [dbo].[{queueName}]"), "Queue not created correctly.");
			}
		}

		[Test]
		[Description("Tests item enqueue.")]
		public void Queue_Enqueue_Success()
		{
			using (var transactionScope = new TransactionScope())
			{
				string queueName = "Queue";

				var queue = new SqlQueue<int>(ConnectionString, "dbo", queueName);

				queue.Enqueue(345);

				var items = SqlHelper.Query<int>(ConnectionString, $"SELECT * FROM [dbo].[{queueName}]", (reader) =>
				{
					return Convert.ToInt32(JsonConvert.DeserializeObject<int>(reader.GetString(4)));
				});

				Assert.AreEqual(1, items.Count, "Wrong item collection.");
				Assert.AreEqual(345, items[0], "Wrong item.");
			}
		}

		[Test]
		[Description("Tests item peek.")]
		public void Queue_Peek_Success()
		{
			using (var transactionScope = new TransactionScope())
			{
				string queueName = "Queue";

				var queue = new SqlQueue<int>(ConnectionString, "dbo", queueName);

				queue.Enqueue(345);

				var items = SqlHelper.Query<int>(ConnectionString, $"SELECT * FROM [dbo].[{queueName}]", (reader) =>
				{
					return Convert.ToInt32(JsonConvert.DeserializeObject<int>(reader.GetString(4)));
				});

				Assert.AreEqual(1, items.Count, "Wrong item collection.");
				Assert.AreEqual(345, items[0], "Wrong item.");

				int peekedElement;

				long? id = queue.Peek(out peekedElement);

				Assert.IsNotNull(id, "Id is null after peek.");
				Assert.IsTrue(id.Value > 0, "Id <= 0");
			}
		}
	}
}